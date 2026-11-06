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
 * Which input of a hash join, its master or its slave, the operator hashes; it probes the hash with the other one.
 * An outer join hashes the input it NULL-extends.
 */
public enum BuildSide {
    /**
     * The slave, with the master fixed as the input that drives the light INNER hash join, so it emits rows in the
     * master's order, which a consumer of the join reads.
     */
    FIXED_MASTER,
    MASTER,
    SLAVE,
    /**
     * The input whose cursor reports fewer rows when the light hash join opens, and the slave when either cursor
     * cannot tell or both report as many; operator planning records it only over a master that supports random
     * access.
     */
    SMALLER;

    /**
     * The input to hash so that the hash holds the fewer rows, {@code tie} when both inputs hold as many rows, and
     * {@link #SMALLER} when either row count is unknown, -1.
     */
    public static BuildSide smaller(long masterRows, long slaveRows, BuildSide tie) {
        if (masterRows < 0 || slaveRows < 0) {
            return SMALLER;
        }
        return masterRows < slaveRows ? MASTER : masterRows > slaveRows ? SLAVE : tie;
    }
}
