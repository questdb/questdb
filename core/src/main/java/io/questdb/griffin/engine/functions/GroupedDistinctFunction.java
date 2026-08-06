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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;

/**
 * Contract for exact DISTINCT aggregates that can use the grouped-distinct physical phase.
 * <p>
 * The phase deduplicates a flat key made of the GROUP BY key columns followed by the
 * distinct argument. Once pair shards have been merged, each unique pair contributes to an
 * ordinary group state through {@link #incrementDistinctValue(MapValue)}. Implementations
 * keep SQL type/null semantics here while the execution engine owns partitioning and maps.
 * When the query also has ordinary aggregates, the implementation must reserve auxiliary
 * flat-path storage for the state-presence marker; nested aggregation does not run at the
 * same time, so its pointer/inline-value slot may be reused.
 */
public interface GroupedDistinctFunction extends GroupByFunction {

    int getDistinctKeyType();

    Function getDistinctKeyFunction();

    long getDistinctValue(MapValue value);

    void incrementDistinctValue(MapValue value);

    boolean isDistinctKeyNull(MapRecord record, int columnIndex);

    boolean isGroupedDistinctStatePresent(MapValue value);

    void mergeDistinctValue(MapValue destValue, MapValue srcValue);

    /**
     * Adds an already-finalized DISTINCT contribution after a row-derived state has
     * been copied over a count-only placeholder.
     */
    void mergeDistinctValue(MapValue destValue, long distinctValue);

    void setGroupedDistinctStatePresent(MapValue value, boolean present);
}
