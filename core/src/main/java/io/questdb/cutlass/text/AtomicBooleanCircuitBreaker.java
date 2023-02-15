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

package io.questdb.cutlass.text;

import io.questdb.cairo.sql.ExecutionCircuitBreaker;

import java.util.concurrent.atomic.AtomicBoolean;

public class AtomicBooleanCircuitBreaker implements ExecutionCircuitBreaker {
    private final AtomicBoolean canceledFlag = new AtomicBoolean(false);

    public void cancel() {
        canceledFlag.set(true);
    }

    @Override
    public boolean checkIfTripped() {
        return isCanceled();
    }

    public boolean isCanceled() {
        return canceledFlag.get();
    }

    public void reset() {
        canceledFlag.set(false);
    }
}
