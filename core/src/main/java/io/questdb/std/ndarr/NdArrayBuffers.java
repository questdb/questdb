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

package io.questdb.std.ndarr;

import io.questdb.cairo.ColumnType;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.bytes.DirectByteSink;

/**
 * Buffers required for an array.
 * <p>The buffers are allocated on first use.</p>
 */
public class NdArrayBuffers implements QuietCloseable {
    public final DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
    public final DirectIntList strides = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
    public final DirectByteSink values = new DirectByteSink(0, MemoryTag.NATIVE_ND_ARRAY);
    public int type = 0;

    @Override
    public void close() {
        Misc.free(shape);
        Misc.free(strides);
        Misc.free(values);
    }

    public void reset() {
        type = ColumnType.UNDEFINED;
        shape.clear();
        strides.clear();
        values.clear();
    }

    /**
     * Validate the buffers and set the array view.
     */
    public NdArrayView.ValidatonStatus setView(NdArrayView view) {
        if (shape.size() == 0) {
            view.ofNull();
            return NdArrayView.ValidatonStatus.OK;
        } else {
            return view.of(
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
