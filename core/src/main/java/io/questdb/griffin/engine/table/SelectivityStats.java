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

package io.questdb.griffin.engine.table;

import io.questdb.std.Mutable;

/**
 * Per-worker selectivity statistics for late materialization heuristics.
 * <p>
 * This class tracks the historical selectivity (ratio of filtered rows to total rows)
 * using exponential moving average (EMA). Late materialization is enabled only when
 * the selectivity is below a threshold (20%), meaning most rows are filtered out.
 * <p>
 * Late materialization benefits:
 * - Low selectivity -> most rows filtered -> avoid decoding unused column data
 * - Only applicable to Parquet format (columnar storage requiring decoding)
 */
public class SelectivityStats implements Mutable {
    // EMA smoothing factor (0.3 favors recent results)
    private static final double ALPHA = 0.3;
    private static final int MIN_SAMPLES = 2;
    // Selectivity threshold (20%) - enable late materialization when selectivity is below this
    private static final double SELECTIVITY_THRESHOLD = 0.2;
    // Exponential moving average of historical selectivity
    private double avgSelectivity = 0.0;
    // Number of samples collected
    private int sampleCount = 0;

    @Override
    public void clear() {
        avgSelectivity = 0.0;
        sampleCount = 0;
    }

    public boolean shouldUseLateMaterialization() {
        if (sampleCount < MIN_SAMPLES) {
            return true;
        }
        return avgSelectivity < SELECTIVITY_THRESHOLD;
    }

    public void update(long filteredRowCount, long totalRowCount) {
        if (totalRowCount <= 0) {
            return;
        }

        double selectivity = (double) filteredRowCount / totalRowCount;

        if (sampleCount == 0) {
            avgSelectivity = selectivity;
        } else {
            // Exponential moving average: EMA = α * current + (1 - α) * EMA_prev
            avgSelectivity = ALPHA * selectivity + (1 - ALPHA) * avgSelectivity;
        }
        sampleCount++;
    }
}
