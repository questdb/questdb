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

package io.questdb.std;

public class Long256Util {

    public static void add(Long256 dst, long v0, long v1, long v2, long v3) {
        boolean isNull = v0 == Numbers.LONG_NULL
                && v1 == Numbers.LONG_NULL
                && v2 == Numbers.LONG_NULL
                && v3 == Numbers.LONG_NULL;

        if (isNull) {
            dst.setAll(
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL
            );
        } else {
            // The sum will overflow if both top bits are set (x & y) or if one of them
            // is (x | y), and a carry from the lower place happened. If such a carry
            // happens, the top bit will be 1 + 0 + 1 = 0 (& ~sum).
            long carry = 0;
            final long l0 = v0 + dst.getLong0() + carry;
            carry = ((v0 & dst.getLong0()) | ((v0 | dst.getLong0()) & ~l0)) >>> 63;

            final long l1 = v1 + dst.getLong1() + carry;
            carry = ((v1 & dst.getLong1()) | ((v1 | dst.getLong1()) & ~l1)) >>> 63;

            final long l2 = v2 + dst.getLong2() + carry;
            carry = ((v2 & dst.getLong2()) | ((v2 | dst.getLong2()) & ~l2)) >>> 63;

            final long l3 = v3 + dst.getLong3() + carry;
            //carry = ((v3 & dst.getLong3()) | ((v3 | dst.getLong3()) & ~l3)) >>> 63;

            dst.setAll(l0, l1, l2, l3);
        }
    }

    public static void add(Long256 acc, Long256 incr) {
        add(acc, incr.getLong0(), incr.getLong1(), incr.getLong2(), incr.getLong3());
    }

    // this method is used by byte-code generator
    public static int compare(Long256 a, Long256 b) {
        if (a.getLong3() < b.getLong3()) {
            return -1;
        }

        if (a.getLong3() > b.getLong3()) {
            return 1;
        }

        if (a.getLong2() < b.getLong2()) {
            return -1;
        }

        if (a.getLong2() > b.getLong2()) {
            return 1;
        }

        if (a.getLong1() < b.getLong1()) {
            return -1;
        }

        if (a.getLong1() > b.getLong1()) {
            return 1;
        }

        return Long.compare(a.getLong0(), b.getLong0());
    }
}
