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

package io.questdb.cairo;

/**
 * Selects which sort backend the SAMPLE BY FILL fast path puts above the
 * GROUP BY output. The fill cursor requires a timestamp-sorted base, and
 * four implementations exist with different memory/compute trade-offs.
 * The default LIGHT_ENCODED is the cheapest when the base supports random
 * access and the sort key encodes -- both hold for a single TIMESTAMP key
 * over Async/GroupBy output.
 */
public final class SampleBySortStrategy {
    public static final int FULL_ENCODED = 1;
    public static final int FULL_RECORDCHAIN = 3;
    public static final int LIGHT_ENCODED = 0;
    public static final int LIGHT_RECORDCHAIN = 2;

    private SampleBySortStrategy() {
    }

    public static String toString(int strategy) {
        switch (strategy) {
            case LIGHT_ENCODED:
                return "light_encoded";
            case FULL_ENCODED:
                return "full_encoded";
            case LIGHT_RECORDCHAIN:
                return "light_recordchain";
            case FULL_RECORDCHAIN:
                return "full_recordchain";
        }
        return "unknown";
    }
}
