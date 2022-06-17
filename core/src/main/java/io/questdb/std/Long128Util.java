/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

public class Long128Util {

    // this method is used by byte-code generator
    public static int compare(Long128 a, Long128 b) {

        if (a.getLong1() < b.getLong1()) {
            return -1;
        }

        if (a.getLong1() > b.getLong1()) {
            return 1;
        }

        return Long.compare(a.getLong0(), b.getLong0());
    }

    public static void add(Long128 dst, long v0, long v1) {
        boolean isNull = v0 == Numbers.LONG_NaN &&
                v1 == Numbers.LONG_NaN;

        if (isNull) {
            dst.setAll(Numbers.LONG_NaN,
                    Numbers.LONG_NaN);
        } else {
            // The sum will overflow if both top bits are set (x & y) or if one of them
            // is (x | y), and a carry from the lower place happened. If such a carry
            // happens, the top bit will be 1 + 0 + 1 = 0 (& ~sum).
            long carry = 0;
            final long l0 = v0 + dst.getLong0() + carry;
            carry = ((v0 & dst.getLong0()) | ((v0 | dst.getLong0()) & ~l0)) >>> 63;

            final long l1 = v1 + dst.getLong1() + carry;

            dst.setAll(l0, l1);
        }
    }

    public static void add(Long128 acc, Long256 incr) {
        add(acc, incr.getLong0(), incr.getLong1());
    }
}
