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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;

/**
 * Immutable lookup backing, borrowed from its builder for one execution. Publish
 * this object through the frame-task publication barrier before probing. The owner
 * must drain all probes and finish reading output symbols before closing the build.
 * Handles, records, symbol tables and their flyweights expire together at close.
 * A later partitioned implementation can route keys without changing this contract.
 */
public interface FrozenHashJoinBuild {
    long getKeyCount();

    long getRowCount();

    /** Allocated native bytes, including unused capacity. */
    long getSizeInBytes();

    /** Each acquired execution slot needs its own probe and circuit breaker. */
    Probe newProbe(SqlExecutionCircuitBreaker circuitBreaker);

    interface Probe extends SymbolTableSource {
        /** Replaces the current duplicate iterator, including on a miss. */
        void find(int key);

        Record getRecord();

        boolean hasNext();

        /** Advances the payload record and returns an opaque, execution-local handle. */
        long next();

        /** Positions the payload record without changing the duplicate iterator. */
        void recordAt(long handle);
    }
}
