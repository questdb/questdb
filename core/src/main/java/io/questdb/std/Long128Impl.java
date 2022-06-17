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

import io.questdb.std.str.CharSink;

public class Long128Impl implements Long128, Sinkable {

    public static final Long128Impl NULL_LONG128 = new Long128Impl();
    public static final Long128Impl ZERO_LONG128 = new Long128Impl();

    private long l0;
    private long l1;

    public void copyFrom(Long128 value) {
        this.l0 = value.getLong0();
        this.l1 = value.getLong1();
    }

    @Override
    public long getLong0() {
        return l0;
    }

    @Override
    public long getLong1() {
        return l1;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj) {
        Long128 that = (Long128) obj;
        return l0 == that.getLong0() && l1 == that.getLong1();
    }

    public void setAll(long l0, long l1) {
        this.l0 = l0;
        this.l1 = l1;
    }

    @Override
    public void toSink(CharSink sink) {
        Numbers.appendLong128(l0, l1, sink);
    }


    static {
        NULL_LONG128.setAll(
                Numbers.LONG_NaN,
                Numbers.LONG_NaN
        );
        ZERO_LONG128.setAll(0, 0);
    }
}
