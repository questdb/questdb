/*******************************************************************************
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

package io.questdb.std.json;

import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

public class SimdJsonResult implements QuietCloseable {
    private static final int JSON_RESULT_STRUCT_SIZE = 12;
    private static final int JSON_RESULT_STRUCT_TYPE_OFFSET = 4;
    private static final int JSON_RESULT_STRUCT_NUMBER_TYPE_OFFSET = JSON_RESULT_STRUCT_TYPE_OFFSET + 4;
    private long impl;

    public SimdJsonResult() {
        this.impl = Unsafe.calloc(JSON_RESULT_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        if (impl != 0) {
            Unsafe.free(impl, JSON_RESULT_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        impl = 0;
    }

    // See constants in `SimdJsonError` for possible values.
    public int getError() {
        return Unsafe.getUnsafe().getInt(impl);
    }

    // See constants in `SimdJsonNumberType` for possible values.
    public int getNumberType() {
        return Unsafe.getUnsafe().getInt(impl + JSON_RESULT_STRUCT_NUMBER_TYPE_OFFSET);
    }

    // See constants in `SimdJsonType` for possible values.
    public int getType() {
        return Unsafe.getUnsafe().getInt(impl + JSON_RESULT_STRUCT_TYPE_OFFSET);
    }

    /**
     * Is not an error (e.g. NO_SUCH_FIELD) and is not a null
     */
    public boolean hasValue() {
        return getError() == SimdJsonError.SUCCESS;
    }

    public boolean isNull() {
        return getType() == SimdJsonType.NULL;
    }

    public long ptr() {
        return impl;
    }
}
