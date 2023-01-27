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

package io.questdb.cutlass.http;

import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public class HttpRangeParser {
    private static final String BYTES = "bytes=";
    private long hi;
    private long lo;

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    public boolean of(CharSequence range) {
        if (!Chars.startsWith(range, BYTES)) {
            return false;
        }

        int n = Chars.indexOf(range, BYTES.length(), '-');

        if (n == -1) {
            return false;
        }

        try {
            this.lo = Numbers.parseLong(range, BYTES.length(), n);
            if (n == range.length() - 1) {
                this.hi = Long.MAX_VALUE;
            } else {
                this.hi = Numbers.parseLong(range, n + 1, range.length());
            }
            return true;
        } catch (NumericException e) {
            return false;
        }
    }
}
