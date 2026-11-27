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
 * by computing them again, or not at all.
 */
public enum RowCopy {
    /**
     * Replays the rows after a rewind. Past its memory budget the copy releases the rows and reads the plan again.
     */
    REPLAY,
    /**
     * Gives the rows row ids and replays them after a rewind. Past its memory budget the copy fails the query.
     */
    RANDOM_ACCESS,
    /**
     * As {@link #RANDOM_ACCESS}, for a consumer that reads every row: the copy takes the rows in batches ahead of the
     * reads.
     */
    RANDOM_ACCESS_IN_BATCHES;

    public boolean isRandomAccess() {
        return this != REPLAY;
    }
}
