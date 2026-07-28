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
        // A single shared static instance had no reset hook at all, and callers stamp state onto
        // a caught CairoException in place - SqlCompilerImpl sets the statement position on the
        // CREATE TABLE AS SELECT path - so one such stamp stuck for the life of the process, on
        // every thread at once. Recycle per carrier and reset, matching the sibling pool
        // exceptions: that keeps the throw allocation-free while confining the state to one
        // carrier and clearing it on every use.
        // Reset to errno 0, not NON_CRITICAL. The old static instance came from the default
        // constructor and never assigned errno, so isCritical() reported true; passing
        // NON_CRITICAL here would quietly flip that, demoting the log level at four call sites and
        // changing the QWP reply from INTERNAL_ERROR to NOT_ACCEPTING_WRITES. Reclassifying a
        // pool-closed error is a separate decision from fixing the shared-state leak.
        ex.clear(0);
        return ex;
    }
}
