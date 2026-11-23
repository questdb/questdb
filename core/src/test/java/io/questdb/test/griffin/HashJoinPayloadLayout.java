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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.std.Rnd;
import io.questdb.test.cairo.Overrides;

/**
 * The two ways a fused hash join GROUP BY's probes read the build's payload columns. The operator
 * picks one per execution from the build's and the probe's row counts; a differential suite forces
 * one through the payload copy properties, picked at random per test, so that across runs every
 * query it checks reads both the columns where they live and the copy.
 */
public enum HashJoinPayloadLayout {
    // Probes read each payload column at the matched build row's id.
    ROW_IDS {
        @Override
        public void apply(Overrides overrides) {
            overrides.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 0);
        }
    },
    // Every build copies its payload columns after it freezes, and probes read the copy.
    COPIED {
        @Override
        public void apply(Overrides overrides) {
            overrides.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, Long.MAX_VALUE);
            overrides.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
            overrides.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        }
    };

    /**
     * A payload layout drawn from the test's generator, whose seeds the test logs.
     */
    public static HashJoinPayloadLayout random(Rnd rnd) {
        return values()[rnd.nextInt(values().length)];
    }

    public abstract void apply(Overrides overrides);
}
