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

import io.questdb.cairo.arr.ArrayState;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.Mutable;
import io.questdb.std.str.CharSink;

import java.util.Arrays;

public class HttpResponseArrayState implements ArrayState, Mutable {
    private final int[] contender = new int[STATE_MAX];
    private final int[] target = new int[STATE_MAX];
    // array view that was partially sent
    private ArrayView arrayView;
    private int flatIndex;
    private HttpChunkedResponse response;

    @Override
    public void clear() {
        this.flatIndex = 0;
        Arrays.fill(contender, 0);
        Arrays.fill(target, 0);
        arrayView = null;
    }

    public ArrayView getArrayView() {
        return arrayView;
    }

    public boolean isClear() {
        return arrayView == null;
    }

    @Override
    public boolean notRecorded(int flatIndex) {
        return this.flatIndex <= flatIndex;
    }

    public void of(HttpChunkedResponse response) {
        this.response = response;
    }

    @Override
    public void putAsciiIfNotRecorded(int eventType, CharSink<?> sink, char symbol) {
        if (++contender[eventType] > target[eventType]) {
            sink.put(symbol);
            response.bookmark();
            target[eventType] = contender[eventType];
        }
    }

    @Override
    public void record(int flatIndex) {
        response.bookmark();
        this.flatIndex = flatIndex;
    }

    public void reset(ArrayView arrayView) {
        Arrays.fill(contender, 0);
        this.arrayView = arrayView;
    }

    /**
     * Zero state is when nothing has been successfully sent to the buffer.
     *
     * @return false when something has been written to the buffer, anything.
     */
    public boolean zeroState() {
        long sum = flatIndex;
        for (int i = 0, n = target.length; i < n; i++) {
            sum += target[i];
            if (sum > 0) {
                return false;
            }
        }
        return sum == 0;
    }
}
