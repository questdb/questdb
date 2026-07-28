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

public class EntryLockedException extends CairoException {
    private static final CarrierLocal<EntryLockedException> tlException = new CarrierLocal<>(EntryLockedException::new);

    public static EntryLockedException instance(CharSequence reason) {
        EntryLockedException ex = tlException.get();
        // Reset through clear() rather than by hand: this is a recycled per-carrier flyweight, so
        // flags, messagePosition and the native backtrace all have to go back to their defaults.
        // Callers stamp state onto a caught CairoException in place - SqlCompilerImpl sets the
        // statement position on the CREATE TABLE AS SELECT path - and without the full reset that
        // state reappears on the next table-busy error raised on the same carrier.
        ex.clear(CairoException.NON_CRITICAL);
        ex.put("table busy [reason=").put(reason).put("]");
        return ex;
    }
}
