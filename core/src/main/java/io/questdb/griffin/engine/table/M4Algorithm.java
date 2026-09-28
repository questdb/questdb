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

import io.questdb.std.DirectLongList;

/**
 * M4 downsampling algorithm: selects up to 4 points per time bucket
 * (first, last, min, max).
 * <p>
 * The bucket walk itself lives in {@link AbstractTimeBucketAlgorithm}, shared
 * with {@link MinMaxAlgorithm}; M4 differs only in emitting all four candidate
 * rows rather than just the two extremes.
 * <p>
 * Reference: Jugel, U. et al. (2014). "M4: A Visualization-Oriented Time
 * Series Data Aggregation." PVLDB Vol. 7, No. 10.
 *
 * @see SubsampleAlgorithm
 * @see AbstractTimeBucketAlgorithm
 */
public class M4Algorithm extends AbstractTimeBucketAlgorithm {
    public static final M4Algorithm INSTANCE = new M4Algorithm();

    @Override
    protected void emitBucket(DirectLongList out, int firstIdx, int minIdx, int maxIdx, int lastIdx) {
        // Sort 4 indices with a sorting network (5 comparisons, 0 allocations)
        // and deduplicate.
        emitSorted4(out, firstIdx, minIdx, maxIdx, lastIdx);
    }

    @Override
    protected int pointsPerBucket() {
        return 4;
    }

    /**
     * Sort 4 values with a sorting network and add unique values to the list.
     */
    static void emitSorted4(DirectLongList out, int a, int b, int c, int d) {
        if (a > b) {
            int t = a;
            a = b;
            b = t;
        }
        if (c > d) {
            int t = c;
            c = d;
            d = t;
        }
        if (a > c) {
            int t = a;
            a = c;
            c = t;
        }
        if (b > d) {
            int t = b;
            b = d;
            d = t;
        }
        if (b > c) {
            int t = b;
            b = c;
            c = t;
        }
        out.add(a);
        if (b != a) {
            out.add(b);
        }
        if (c != b) {
            out.add(c);
        }
        if (d != c) {
            out.add(d);
        }
    }

}
