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

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

public class HttpRangeParser {
    private static final Utf8String BYTES = new Utf8String("bytes=");
    private long hi;
    private long lo;

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    public boolean of(DirectUtf8Sequence range) {
        if (!Utf8s.startsWith(range, BYTES)) {
            return false;
        }

        int n = Utf8s.indexOfAscii(range, BYTES.size(), '-');
        if (n == -1) {
            return false;
        }

        try {
            this.lo = Numbers.parseLong(range, BYTES.size(), n);
            if (n == range.size() - 1) {
                this.hi = Long.MAX_VALUE;
            } else {
                this.hi = Numbers.parseLong(range, n + 1, range.size());
            }
            return true;
        } catch (NumericException e) {
            return false;
        }
    }
}
