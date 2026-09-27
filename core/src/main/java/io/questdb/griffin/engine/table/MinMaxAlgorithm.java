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
 * MinMax downsampling algorithm: selects up to 2 points per time bucket
 * (min and max value).
 * <p>
 * Shares {@link AbstractTimeBucketAlgorithm}'s bucket walk with {@link M4Algorithm}
 * and simply discards the bucket's first/last rows, so the output is half the
 * size of M4's. Best for simple envelope visualization where first/last
 * positions within the bucket don't matter.
 *
 * @see SubsampleAlgorithm
 * @see AbstractTimeBucketAlgorithm
 */
public class MinMaxAlgorithm extends AbstractTimeBucketAlgorithm {
    public static final MinMaxAlgorithm INSTANCE = new MinMaxAlgorithm();

    @Override
    protected void emitBucket(DirectLongList out, int firstIdx, int minIdx, int maxIdx, int lastIdx) {
        // Emit in timestamp order, deduplicated; first/last are deliberately ignored.
        SubsampleAlgorithm.emitAscendingPair(out, minIdx, maxIdx);
    }

    @Override
    protected int pointsPerBucket() {
        return 2;
    }
}
