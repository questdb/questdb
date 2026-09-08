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

package io.questdb.test.tools;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * A filter that holds native memory, so dropping it fails the leak check, and counts its closes, so
 * closing it twice fails an assertion instead of silently double-freeing. Over-nulling is equally
 * observable: it both leaks the buffer and leaves {@link #closeCount} at zero.
 */
public class NativeFilter extends BooleanFunction {
    private static final long SIZE = 64;
    public int closeCount;
    private long ptr;

    public NativeFilter() {
        ptr = Unsafe.malloc(SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        closeCount++;
        if (ptr != 0) {
            ptr = Unsafe.free(ptr, SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Override
    public boolean getBool(Record rec) {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("NativeFilter");
    }
}
