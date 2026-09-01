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

package io.questdb.cairo.lv;

import io.questdb.cairo.RecordSink;
import org.jetbrains.annotations.NotNull;

/**
 * Wraps the reader-local half of {@link LiveViewCheckpointKeyProjector}'s two sinks -
 * {@link LiveViewCheckpointKeyProjector#getKeySink()} - so it is not assignment-compatible
 * with {@link LiveViewCheckpointKeySink}, its checkpoint-domain counterpart.
 * <p>
 * Today a reader-local SYMBOL key is a table-local int and a checkpoint-domain one is a
 * resolved STRING, so a sink mix-up fails immediately as a schema mismatch. Once a
 * translated term keys both as SYMBOL - an LV-private id stable across the view's whole
 * history rather than one pinned reader's lifetime - the two become structurally
 * identical ints over the same value range, and nothing but this wrapper would catch one
 * being handed to the map that expects the other. The wrapper does not remove the mix-up
 * an existing call site would still make by naming the wrong accessor; it stops one from
 * surviving a variable handed across a wider boundary, such as a helper refactored to
 * take "a key sink" instead of "the reader-local key sink".
 */
public final class LiveViewReaderLocalKeySink {
    private final RecordSink sink;

    LiveViewReaderLocalKeySink(@NotNull RecordSink sink) {
        this.sink = sink;
    }

    public @NotNull RecordSink unwrap() {
        return sink;
    }
}
