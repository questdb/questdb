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
 * How a join step combines its input with the rows of the inputs before it.
 */
public enum JoinKind {
    CROSS,
    INNER,
    LEFT_OUTER,
    RIGHT_OUTER,
    FULL_OUTER,
    ASOF,
    LT,
    SPLICE,
    UNNEST;

    /**
     * True when the step does not commute with the steps around it: every kind but INNER and CROSS.
     */
    public boolean isBarrier() {
        return this != INNER && this != CROSS;
    }

    /**
     * True when the step can emit rows whose master columns are NULL-extended.
     */
    public boolean isMasterNulling() {
        return this == RIGHT_OUTER || this == FULL_OUTER || this == SPLICE;
    }

    /**
     * True when the step can emit rows whose own columns are NULL-extended.
     */
    public boolean isSlaveNulling() {
        return this == LEFT_OUTER || this == FULL_OUTER || this == ASOF || this == LT || this == SPLICE;
    }

    /**
     * True when the step matches rows by designated timestamp.
     */
    public boolean isTemporal() {
        return this == ASOF || this == LT || this == SPLICE;
    }
}
