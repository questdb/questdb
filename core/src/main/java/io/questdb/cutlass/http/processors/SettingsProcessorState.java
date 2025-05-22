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

package io.questdb.cutlass.http.processors;

import io.questdb.preferences.SettingsStore;
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

class SettingsProcessorState implements Mutable, Closeable {
    final StringSink utf16Sink;
    final DirectUtf8Sink utf8Sink;
    SettingsStore.Mode mode;

    SettingsProcessorState(int size) {
        utf8Sink = new DirectUtf8Sink(size);
        utf16Sink = new StringSink();
    }

    @Override
    public void clear() {
        utf8Sink.clear();
        utf16Sink.clear();
        mode = null;
    }

    @Override
    public void close() {
        utf8Sink.close();
    }
}
