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

package io.questdb.cairo.arr;

import io.questdb.cairo.ColumnType;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.bytes.DirectByteSink;
import org.jetbrains.annotations.NotNull;

/**
 * Buffers required for an array. The buffers are allocated on first use. Use this when parsing/loading a new array.
 * If you only need a buffer for the strides, use the more specialized {@link ArrayMmapBuffer} instead.
 */
public class ArrayBuffers implements QuietCloseable {
    public final DirectIntList currCoords = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG2);
    public final DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG2);
    public final DirectIntList strides = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG2);
    public final DirectByteSink values = new DirectByteSink(0, MemoryTag.NATIVE_ND_ARRAY_DBG2);
    public int type = 0;

    @Override
    public void close() {
        Misc.free(currCoords);
        Misc.free(shape);
        Misc.free(strides);
        Misc.free(values);
    }

    public void reset() {
        type = ColumnType.UNDEFINED;
        currCoords.clear();
        shape.clear();
        strides.clear();
        values.clear();
    }

    /**
     * Validates the buffers and updates the array view.
     */
    public void updateView(@NotNull DirectFlyweightArrayView view) {
        if (shape.size() == 0) {
            view.ofNull();
        } else {
            view.of(
                    type,
                    shape.getAddress(),
                    (int) shape.size(),
                    strides.getAddress(),
                    (int) strides.size(),
                    values.ptr(),
                    values.size(),
                    0
            );
        }
    }
}
