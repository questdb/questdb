/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.tasks;

import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.engine.groupby.MasterRowBatch;
import io.questdb.griffin.engine.table.AsyncMarkoutGroupByAtom;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.Mutable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Task for parallel markout query execution.
 * Each task contains a batch of master rows to be processed through the
 * MarkoutHorizon -> ASOF JOIN -> GROUP BY pipeline.
 */
public class MarkoutReduceTask implements Mutable {
    // Coordination primitives
    private AtomicBooleanCircuitBreaker circuitBreaker;
    private AtomicInteger startedCounter;
    private CountDownLatchSPI doneLatch;

    // Task identification
    private int batchIndex = -1;

    // Master row batch - materialized master rows for this task
    private MasterRowBatch masterRowBatch;

    // Reference to the atom (holds shared state and per-worker resources)
    private AsyncMarkoutGroupByAtom atom;

    @Override
    public void clear() {
        circuitBreaker = null;
        startedCounter = null;
        doneLatch = null;
        batchIndex = -1;
        masterRowBatch = null;
        atom = null;
    }

    public void of(
            AtomicBooleanCircuitBreaker circuitBreaker,
            AtomicInteger startedCounter,
            CountDownLatchSPI doneLatch,
            AsyncMarkoutGroupByAtom atom,
            int batchIndex,
            MasterRowBatch masterRowBatch
    ) {
        this.circuitBreaker = circuitBreaker;
        this.startedCounter = startedCounter;
        this.doneLatch = doneLatch;
        this.atom = atom;
        this.batchIndex = batchIndex;
        this.masterRowBatch = masterRowBatch;
    }

    public AtomicBooleanCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public AtomicInteger getStartedCounter() {
        return startedCounter;
    }

    public CountDownLatchSPI getDoneLatch() {
        return doneLatch;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public MasterRowBatch getMasterRowBatch() {
        return masterRowBatch;
    }

    public AsyncMarkoutGroupByAtom getAtom() {
        return atom;
    }
}
