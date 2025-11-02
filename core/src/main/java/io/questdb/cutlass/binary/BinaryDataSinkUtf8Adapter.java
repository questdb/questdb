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

package io.questdb.cutlass.binary;

import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;

/**
 * Adapter that allows BinaryDataSink to be used as a Utf8Sink.
 * This enables use of Utf8s.encodeUtf16WithLimit() for efficient UTF-8 encoding.
 * The adapter is reusable - call of() to set a new sink.
 */
public class BinaryDataSinkUtf8Adapter implements Utf8Sink {
    private BinaryDataSink delegate;

    public BinaryDataSinkUtf8Adapter() {
    }

    public void of(BinaryDataSink sink) {
        this.delegate = sink;
    }

    @Override
    public Utf8Sink put(byte b) {
        delegate.putByte(b);
        return this;
    }

    @Override
    public Utf8Sink put(Utf8Sequence us) {
        if (us != null) {
            int size = us.size();
            for (int i = 0; i < size; i++) {
                delegate.putByte(us.byteAt(i));
            }
        }
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        // Write UTF-8 bytes from memory range [lo, hi)
        long len = hi - lo;
        for (long i = 0; i < len; i++) {
            delegate.putByte(Unsafe.getUnsafe().getByte(lo + i));
        }
        return this;
    }
}
