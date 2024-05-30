/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;

public class JsonResult implements QuietCloseable {
    private long impl;

    public JsonResult() {
        this.impl = Unsafe.calloc(12, MemoryTag.NATIVE_DEFAULT);
    }

    // See constants in `JsonError` for possible values.
    public int getError() {
        return Unsafe.getUnsafe().getInt(impl);
    }

    // See constants in `JsonType` for possible values.
    public int getType() {
        return Unsafe.getUnsafe().getInt(impl + 4);
    }

    // See constants in `JsonNumType` for possible values.
    public int getNumType() {
        return Unsafe.getUnsafe().getInt(impl + 8);
    }

    public void clear() {
        Unsafe.getUnsafe().putInt(impl, 0);
        Unsafe.getUnsafe().putInt(impl + 4, 0);
        Unsafe.getUnsafe().putInt(impl + 8, 0);
    }

    @Override
    public void close() {
        if (impl != 0) {
            Unsafe.free(impl, 12, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /** Is not an error (e.g. NO_SUCH_FIELD) and is not a null */
    public boolean hasValue() {
        return getError() == JsonError.SUCCESS;
    }

    public long ptr() {
        return impl;
    }

    public boolean isNull() {
        return getType() == JsonType.NULL;
    }

    public void throwIfError(String culpritFunctionName, DirectUtf8Sequence path) throws CairoException {
        final int error = getError();
        if (error != JsonError.SUCCESS) {
            throw CairoException.nonCritical()
                    .put(culpritFunctionName)
                    .put("(.., ")
                    .put('\'')
                    .put(path)
                    .put("'): ")
                    .put(JsonError.getMessage(error));
        }
    }
}
