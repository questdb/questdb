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

package io.questdb.test.griffin.engine.groupby.hyperloglog;

import static org.junit.Assert.assertTrue;

public class HyperLogLogTestUtils {

    public static void assertCardinality(int exact, int precision, long estimated) {
        // According to the paper 'HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm.'
        // (https://hal.science/hal-00406166) the estimates provided by HyperLogLog are expected to be within
        // 3 standard errors of the exact count in 99% of all cases. The standard error is around 1.04 / sqrt(2^precision).
        // To mitigate the likelihood of unreliable tests, we opt for a slightly wider range (increasing from 3 to 5 standard errors).
        double standardError = 1.04 / Math.sqrt(1 << precision);
        double min = exact - (5 * standardError) * exact;
        double max = exact + (5 * standardError) * exact;
        assertTrue(
                "Estimated cardinality " + estimated + " for precision " + precision + " is not within the expected range of " + min + " to " + max + ", std error " + standardError,
                estimated >= min && estimated <= max
        );
    }
}
