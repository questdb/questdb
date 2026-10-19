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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.std.NumericException;

/**
 * Encodes the class of a throwable captured on a worker thread so it can be
 * re-built as the same class on the collector thread. Async dispatch can't
 * safely carry the original exception instance across threads (thread-local
 * singletons and mutable flyweight messages), so we record a kind tag and
 * rebuild on the reader side.
 */
public final class AsyncQueryErrorKind {
    public static final byte KIND_CAIRO = 1;
    public static final byte KIND_IMPLICIT_CAST = 2;
    public static final byte KIND_NONE = 0;
    public static final byte KIND_NUMERIC = 3;
    public static final byte KIND_UNEXPECTED = 4;

    private AsyncQueryErrorKind() {
    }

    public static byte of(Throwable th) {
        if (th instanceof CairoException) {
            return KIND_CAIRO;
        }
        if (th instanceof ImplicitCastException) {
            return KIND_IMPLICIT_CAST;
        }
        if (th instanceof NumericException) {
            return KIND_NUMERIC;
        }
        return KIND_UNEXPECTED;
    }
}
