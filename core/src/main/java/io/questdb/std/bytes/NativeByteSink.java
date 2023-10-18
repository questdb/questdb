/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.std.bytes;

import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;

/**
 * A try-with-resource accessor to the `questdb_byte_sink_t` structure.
 * <p>
 * Note that the close method is simply meant to allow the owner object to
 * update its memory bookkeeping. The underlying memory is not released.
 */
public abstract class NativeByteSink implements QuietCloseable {
    public static native long book(long impl, long len);

    public static native long create(long capacity);

    public static native void destroy(long impl);

    /**
     * Get the raw pointer to the `questdb_byte_sink_t` C structure.
     */
    public abstract long ptr();

    static {
        Os.init();
    }
}
