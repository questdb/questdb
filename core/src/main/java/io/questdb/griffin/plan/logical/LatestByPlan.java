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

package io.questdb.griffin.plan.logical;

import io.questdb.std.IntList;
import io.questdb.std.ObjectFactory;

public final class LatestByPlan extends ForwardingPlan {
    public static final ObjectFactory<LatestByPlan> FACTORY = LatestByPlan::new;
    private final IntList keyColumnIds = new IntList();
    private Algorithm algorithm;
    private boolean isTimestampOrderInherited;
    private int timestampColumnId = -1;

    @Override
    public void clear() {
        super.clear();
        keyColumnIds.clear();
        algorithm = null;
        isTimestampOrderInherited = false;
        timestampColumnId = -1;
    }

    /**
     * How the generator finds the latest rows of a derived input, or null before order planning decided it or when
     * the LATEST BY reads a table scan.
     */
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public IntList getKeyColumnIds() {
        return keyColumnIds;
    }

    public int getTimestampColumnId() {
        return timestampColumnId;
    }

    public boolean isTimestampOrderInherited() {
        return isTimestampOrderInherited;
    }

    public LatestByPlan of(LogicalPlan input, int timestampColumnId, int position) {
        configure(input, position);
        this.timestampColumnId = timestampColumnId;
        return this;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public void setTimestampOrderInherited(boolean isTimestampOrderInherited) {
        this.isTimestampOrderInherited = isTimestampOrderInherited;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.columnIds(keyColumnIds, null, visitor);
        timestampColumnId = PlanReads.columnId(timestampColumnId, -1, visitor);
    }

    /**
     * How the generator finds the latest rows of a derived input.
     */
    public enum Algorithm {
        /**
         * Keeps the row id of the latest row of each key of a random-access input.
         */
        LIGHT,
        /**
         * Keeps the row id of the latest row of each key of a random-access input that delivers the rows in ascending
         * timestamp order, so the last row of a key is its latest.
         */
        ASCENDING_LIGHT,
        /**
         * Keeps a copy of the latest row of each key of an input without random access.
         */
        MATERIALIZED
    }
}
