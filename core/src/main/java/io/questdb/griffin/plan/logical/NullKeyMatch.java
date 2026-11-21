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
 * How the factory of a step NULL-extended by its ON residual, see {@link JoinNullExtension#BY_ON_RESIDUAL}, finds the
 * rows that pass the ON residual with a row whose keys are all NULL: the slave rows that pass it with such a master
 * row, and for a RIGHT or FULL step the rows of the NULL key chain that pass it with some master row.
 */
public enum NullKeyMatch {
    /**
     * Evaluates the ON residual on master rows alone: the ON residual reads only the master input and takes the same
     * value for a row on every evaluation, so no slave row changes whether a master row passes. The factory tests a
     * master row whose keys are all NULL once, which some slave row passes with when it passes and the slave input has
     * a row, and matches every row of the NULL key chain once a master row passes; a FULL join factory that hashes its
     * master tests once per execution whether some master row passes instead.
     */
    PER_MASTER_ROW,
    /**
     * Evaluates the ON residual on each pair of a master and a slave row the factory walks.
     */
    PER_PAIR,
    /**
     * Evaluates the ON residual on slave rows alone: the ON residual reads only the slave input and takes the same
     * value for a row on every evaluation, so no master row changes whether a slave row passes. The factory tests
     * once per execution whether some slave row passes, and tests a row of the NULL key chain when it emits the
     * slave rows no master row matched; a FULL join factory that hashes its master tests each slave row it probes
     * the master with instead.
     */
    PER_SLAVE_ROW
}
