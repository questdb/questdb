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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;

/**
 * A window function partitioned by the key of a key-major scan that the parallel window computes
 * one run of a key's rows at a time, without its partition map: see
 * {@link AsyncWindowAtom}. The rows of a run all belong to one key and arrive in the order the
 * serial window sees them, so the state of one partition, kept in the function itself, is all a
 * run needs.
 * <p>
 * The values must be the ones {@link #computeNext(Record)} produces for the same rows of the
 * same partition, bit for bit: a run is the same arithmetic in the same order, only with the
 * partition's state held in fields rather than in a map value.
 */
public interface KeyRunWindowFunction extends WindowFunction {

    /**
     * The argument the function reads off each row, for the reader to know which columns a run
     * loads; null when it is not a single argument.
     */
    Function getKeyRunArgument();

    /**
     * Whether this instance can compute runs. Decided at compile time and constant after.
     */
    boolean isKeyRunSupported();

    /**
     * Computes the next row of the current run, positioned on {@code record}, after which the
     * function's getter returns that row's value.
     */
    void keyRunNext(Record record);

    /**
     * Starts a run: the next row is its partition's first.
     */
    void keyRunStart();
}
