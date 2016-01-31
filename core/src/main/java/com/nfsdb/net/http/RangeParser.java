/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.std.ObjectFactory;

public class RangeParser {
    public static final ObjectFactory<RangeParser> FACTORY = new ObjectFactory<RangeParser>() {
        @Override
        public RangeParser newInstance() {
            return new RangeParser();
        }
    };
    private static final String BYTES = "bytes=";
    private long lo;
    private long hi;

    private RangeParser() {
    }

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
