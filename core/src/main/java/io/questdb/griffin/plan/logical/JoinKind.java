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
    UNNEST,
    /**
     * A key-less CROSS input of the master side of the RIGHT or FULL step it follows, which join ordering moved past
     * that step: each row the step NULL-extends on its master side joins once, with this input's columns NULL, and
     * every other row joins each row of the input. The step's master side holds no row when the input holds none.
     */
    OUTER_CROSS;

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
     * True when the step can emit rows whose own or master columns are NULL-extended.
     */
    public boolean isNulling() {
        return isSlaveNulling() || isMasterNulling();
    }

    /**
     * True when the step is a LEFT, RIGHT or FULL outer join.
     */
    public boolean isOuter() {
        return this == LEFT_OUTER || this == RIGHT_OUTER || this == FULL_OUTER;
    }

    /**
     * True when a key of the step on a master column that another key of the step already keys filters the step's
     * input on the equality of the two keys' slave columns instead, see {@link JoinInput#getKeyFilter}: LEFT, ASOF and
     * LT, which drop the input rows the filter drops. A RIGHT or FULL step keeps them.
     */
    public boolean isRepeatedKeyFiltered() {
        return this == LEFT_OUTER || this == ASOF || this == LT;
    }

    /**
     * True when the step matches rows by its ON condition alone: CROSS, INNER and the outer joins.
     */
    public boolean isRelational() {
        return !isTemporal() && this != UNNEST;
    }

    /**
     * True when the step can emit rows whose own columns are NULL-extended.
     */
    public boolean isSlaveNulling() {
        return this == LEFT_OUTER || this == FULL_OUTER || this == ASOF || this == LT || this == SPLICE || this == OUTER_CROSS;
    }

    /**
     * True when the step matches rows by designated timestamp.
     */
    public boolean isTemporal() {
        return this == ASOF || this == LT || this == SPLICE;
    }
}
