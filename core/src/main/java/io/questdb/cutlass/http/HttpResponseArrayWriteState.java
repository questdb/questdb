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

package io.questdb.cutlass.http;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.ArrayWriteState;
import io.questdb.std.Mutable;
import io.questdb.std.str.CharSink;

public class HttpResponseArrayWriteState implements ArrayWriteState, Mutable {
    // array that is being partially sent, resuming at the correct spot after a retry
    private ArrayView arrayView;
    private int opsAlreadyDone;
    private int opsSinceReset;
    private HttpChunkedResponse response;

    @Override
    public void clear() {
        this.opsSinceReset = 0;
        this.opsAlreadyDone = 0;
        arrayView = null;
    }

    public ArrayView getArrayView() {
        return arrayView;
    }

    @Override
    public boolean incAndSayIfNewOp() {
        opsSinceReset++;
        return opsSinceReset > opsAlreadyDone;
    }

    public boolean isClear() {
        return arrayView == null;
    }

    /**
     * Returns true if nothing has been successfully sent to the buffer.
     */
    public boolean isNothingWritten() {
        return opsAlreadyDone == 0;
    }

    public void of(HttpChunkedResponse response) {
        this.response = response;
    }

    @Override
    public void performedOp() {
        opsAlreadyDone = Math.max(opsAlreadyDone, opsSinceReset);
        response.bookmark();
    }

    @Override
    public void putCharIfNew(CharSink<?> sink, char symbol) {
        if (incAndSayIfNewOp()) {
            sink.put(symbol);
        }
        performedOp();
    }

    public void reset(ArrayView arrayView) {
        this.opsSinceReset = 0;
        this.arrayView = arrayView;
    }
}
