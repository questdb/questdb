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

package io.questdb.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.std.CarrierLocal;

public class LimitOverflowException extends CairoException {
    private static final CarrierLocal<LimitOverflowException> tlException = new CarrierLocal<>(LimitOverflowException::new);

    public static LimitOverflowException instance() {
        LimitOverflowException ex = tlException.get();
        // Reset through clear() rather than by hand: this is a recycled per-carrier flyweight, so
        // flags, messagePosition and the native backtrace all have to go back to their defaults.
        // Callers stamp state onto a caught CairoException in place. SqlCompilerImpl does it on
        // the CREATE TABLE / MATERIALIZED VIEW / VIEW AS SELECT paths, which wrap the cursor copy
        // and so can catch exactly this exception, and on ALTER TABLE RESUME and SUSPEND.
        // compileAlterTable() and compileAlterMatView() stamp only when the position still reads 0,
        // so a stale non-zero one there does not merely linger - it suppresses the position those
        // two would otherwise set. Without the full reset that
        // state reappears on the next limit overflow raised on the same carrier.
        ex.clear(NON_CRITICAL);
        return ex;
    }
}
