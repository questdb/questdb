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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.cairo.Overrides;

import java.util.ArrayList;
import java.util.Collection;

/**
 * The two ways a fused hash join GROUP BY with an INT or SYMBOL key builds. The operator picks one
 * per execution from the build input's row count; a differential suite runs under each, forced
 * through the build properties, so that every query it checks runs both the owner's build and the
 * parallel build's rounds. The parallel mode also shrinks the context's page frames, which the
 * suite restores after each test.
 */
public enum HashJoinBuildMode {
    // Every build walks its frames on the query's own thread.
    SERIAL {
        @Override
        public void applyProperties(Overrides overrides) {
            overrides.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        }
    },
    // Every build runs in rounds, with as many hash partitions as its rows allow.
    PARALLEL {
        @Override
        public void applyProperties(Overrides overrides) {
            overrides.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            overrides.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
        }
    };

    /** Every payload layout under every build mode. */
    public static Collection<Object[]> parameters() {
        final Collection<Object[]> parameters = new ArrayList<>();
        for (HashJoinPayloadLayout layout : HashJoinPayloadLayout.values()) {
            for (HashJoinBuildMode mode : values()) {
                parameters.add(new Object[]{layout, mode});
            }
        }
        return parameters;
    }

    /**
     * Forces the mode on the operator, and, for the parallel one, shrinks the context's page frames
     * to a few rows, so that a small table's build still spreads over several frames.
     */
    public void apply(Overrides overrides, SqlExecutionContext context) {
        applyProperties(overrides);
        if (this == PARALLEL) {
            context.changePageFrameSizes(2, 3);
        }
    }

    /** Forces the mode through the build properties alone. */
    public abstract void applyProperties(Overrides overrides);
}
