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

package io.questdb.cutlass.http;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.ArrayWriteState;
import io.questdb.std.Mutable;
import io.questdb.std.str.CharSink;

public class HttpResponseArrayWriteState implements ArrayWriteState, Mutable {
    // array view that was partially sent
    private ArrayView arrayView;
    private int flatIndexAlreadyWritten;
    private HttpChunkedResponse response;
    private int symbolsAlreadyWritten;
    private int symbolsSeenSinceRestart;

    @Override
    public void clear() {
        this.flatIndexAlreadyWritten = 0;
        this.symbolsSeenSinceRestart = 0;
        this.symbolsAlreadyWritten = 0;
        arrayView = null;
    }

    public ArrayView getArrayView() {
        return arrayView;
    }

    public boolean isClear() {
        return arrayView == null;
    }

    @Override
    public boolean isNotWritten(int flatIndex) {
        return this.flatIndexAlreadyWritten <= flatIndex;
    }

    /**
     * Zero state is when nothing has been successfully sent to the buffer.
     *
     * @return false when something has been written to the buffer, anything.
     */
    public boolean isNothingWritten() {
        return flatIndexAlreadyWritten + symbolsAlreadyWritten == 0;
    }

    public void of(HttpChunkedResponse response) {
        this.response = response;
    }

    @Override
    public void putAsciiIfNew(CharSink<?> sink, char symbol) {
        if (++symbolsSeenSinceRestart <= symbolsAlreadyWritten) {
            return;
        }
        sink.put(symbol);
        response.bookmark();
        symbolsAlreadyWritten = symbolsSeenSinceRestart;
    }

    public void reset(ArrayView arrayView) {
        this.symbolsSeenSinceRestart = 0;
        this.arrayView = arrayView;
    }

    @Override
    public void wroteFlatIndex(int flatIndex) {
        response.bookmark();
        this.flatIndexAlreadyWritten = flatIndex;
    }
}
