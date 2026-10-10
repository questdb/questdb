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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.TimestampDriver;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;

public interface TimestampCompatibleAdapter extends TypeAdapter {
    long getTimestamp(DirectUtf8Sequence value, TimestampDriver driver) throws Exception;

    /**
     * Parallel COPY workers pass their own UTF-16 sink, as they do to the 6-arg
     * {@link TypeAdapter#write}, because the adapter's own sink belongs to a shared TypeManager.
     */
    default long getTimestamp(DirectUtf8Sequence value, TimestampDriver driver, DirectUtf16Sink utf16Sink) throws Exception {
        return getTimestamp(value, driver);
    }
}
