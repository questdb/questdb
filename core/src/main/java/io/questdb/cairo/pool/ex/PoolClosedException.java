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

package io.questdb.cairo.pool.ex;

import io.questdb.cairo.CairoException;
import io.questdb.std.CarrierLocal;

public class PoolClosedException extends CairoException {
    private static final CarrierLocal<PoolClosedException> tlException = new CarrierLocal<>(PoolClosedException::new);

    public static PoolClosedException instance() {
        PoolClosedException ex = tlException.get();
        // A single shared static instance had no reset hook, and SqlCompilerImpl stamps the
        // statement position onto a caught CairoException in place - so one stamp stuck for the
        // life of the process, on every thread at once. A per-carrier flyweight keeps the throw
        // allocation-free while confining that state to one carrier.
        // Reset to errno 0, not NON_CRITICAL: the old instance never assigned errno, so
        // isCritical() reported true, and NON_CRITICAL would demote the log level at every
        // isCritical() call site and flip the QWP reply to NOT_ACCEPTING_WRITES.
        ex.clear(0);
        return ex;
    }
}
