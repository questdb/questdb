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

/**
 * The copy of the rows of a plan its consumer reads in place of the plan's own cursor, see
 * {@link LogicalPlan#getOutputCopy()}: the consumer reads the rows again or by row id, which the plan alone serves only
 * by computing them again, or not at all. A copy outside the join budget stands in for the map of a hash GROUP BY over
 * rows a stream GROUP BY computes, which only the query memory limit bounds; a copy within it holds rows a join
 * reads, which the join budget bounds. At its limit, a copy that only replays the rows releases them and reads the plan
 * again, which suits a consumer that reads them once more; a copy that gives row ids is strict and fails the query, as
 * its consumer relies on the row ids or reads the rows again for each row of another input, which reading the plan
 * again for each row would multiply.
 */
public enum RowCopy {
    /**
     * Replays the rows after a rewind; past the query memory limit the copy releases the rows and reads the plan again.
     */
    REPLAY(false, false, false),
    /**
     * Gives the rows row ids and replays them after a rewind; past the query memory limit the copy fails the query.
     */
    RANDOM_ACCESS(true, false, false),
    /**
     * As {@link #RANDOM_ACCESS}, for a consumer that reads every row: the copy takes the rows in batches ahead of the
     * reads.
     */
    RANDOM_ACCESS_IN_BATCHES(true, false, true),
    /**
     * Replays the rows of a slave join a nested loop reads again for each master row; past the join budget the copy
     * releases the rows and reads the join again.
     */
    JOIN_CACHE(false, true, false),
    /**
     * Gives row ids to the rows of a join slave without random access; past the join budget the copy fails the query.
     */
    JOIN_RANDOM_ACCESS(true, true, false);

    private final boolean isCopiedInBatches;
    private final boolean isJoinBudgeted;
    private final boolean isRandomAccess;

    RowCopy(boolean isRandomAccess, boolean isJoinBudgeted, boolean isCopiedInBatches) {
        this.isRandomAccess = isRandomAccess;
        this.isJoinBudgeted = isJoinBudgeted;
        this.isCopiedInBatches = isCopiedInBatches;
    }

    public boolean isCopiedInBatches() {
        return isCopiedInBatches;
    }

    public boolean isJoinBudgeted() {
        return isJoinBudgeted;
    }

    public boolean isRandomAccess() {
        return isRandomAccess;
    }

    /**
     * Whether the copy fails the query at its limit rather than reading the plan again: a copy that gives row ids.
     */
    public boolean isStrict() {
        return isRandomAccess;
    }
}
