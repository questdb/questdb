/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;

import java.io.Closeable;

class TextImportProcessorState implements Mutable, Closeable {
    public static final int STATE_OK = 0;
    final TextLoader textLoader;
    public int columnIndex = 0;
    String stateMessage;
    boolean analysed = false;
    int messagePart = TextImportProcessor.MESSAGE_UNKNOWN;
    int responseState = TextImportProcessor.RESPONSE_PREFIX;
    boolean forceHeader = false;
    int state;
    boolean json = false;

    TextImportProcessorState(CairoEngine engine) {
        this.textLoader = new TextLoader(engine);
    }

    @Override
    public void clear() {
        responseState = TextImportProcessor.RESPONSE_PREFIX;
        columnIndex = 0;
        messagePart = TextImportProcessor.MESSAGE_UNKNOWN;
        analysed = false;
        state = STATE_OK;
        textLoader.clear();
    }

    @Override
    public void close() {
        clear();
        Misc.free(textLoader);
    }
}
