/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cutlass.text.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.Mutable;

public class GroupByMergeShardTask implements Mutable {
    private AsyncGroupByAtom atom;
    private AtomicBooleanCircuitBreaker circuitBreaker;
    private CountDownLatchSPI doneLatch;
    private int shardIndex = -1;

    @Override
    public void clear() {
        shardIndex = -1;
        atom = null;
        circuitBreaker = null;
    }

    public AsyncGroupByAtom getAtom() {
        return atom;
    }

    public AtomicBooleanCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public CountDownLatchSPI getDoneLatch() {
        return doneLatch;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public void of(AtomicBooleanCircuitBreaker circuitBreaker, CountDownLatchSPI doneLatch, AsyncGroupByAtom atom, int shardIndex) {
        this.circuitBreaker = circuitBreaker;
        this.doneLatch = doneLatch;
        this.atom = atom;
        this.shardIndex = shardIndex;
    }
}
