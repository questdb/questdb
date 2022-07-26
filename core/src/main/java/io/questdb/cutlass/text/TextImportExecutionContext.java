/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;

import java.util.concurrent.atomic.AtomicLong;

public class TextImportExecutionContext {
    private final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker();
    private final AtomicLong activeImportId = new AtomicLong(-1);
    // Important assumption: We never access the rnd concurrently, so no need for additional synchronization.
    private final Rnd rnd;

    public TextImportExecutionContext(CairoConfiguration configuration) {
        MicrosecondClock clock = configuration.getMicrosecondClock();
        this.rnd = new Rnd(clock.getTicks(), clock.getTicks());
    }

    public AtomicBooleanCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public boolean isActive() {
        return activeImportId.get() > -1;
    }

    public boolean sameAsActiveImportId(long importId) {
        return activeImportId.get() == importId;
    }

    public void resetActiveImportId() {
        activeImportId.set(-1);
    }

    public long assignActiveImportId() {
        long nextId = rnd.nextPositiveLong();
        activeImportId.set(nextId);
        return nextId;
    }
}
