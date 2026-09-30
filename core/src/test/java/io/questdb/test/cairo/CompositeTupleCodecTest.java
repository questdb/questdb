/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.cairo.CompositeTupleCodec;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class CompositeTupleCodecTest {
    @Test
    public void testRoundTripAndInjective() {
        StringSink a = new StringSink();
        CompositeTupleCodec.encode(new int[]{7, 0, -1}, 3, a);
        Assert.assertEquals(24, a.length());               // 3 ints * 8 hex chars
        int[] out = new int[3];
        Assert.assertEquals(3, CompositeTupleCodec.decode(a, out));
        Assert.assertArrayEquals(new int[]{7, 0, -1}, out);

        StringSink b = new StringSink();
        CompositeTupleCodec.encode(new int[]{7, 0, 0}, 3, b);
        Assert.assertNotEquals(a.toString(), b.toString()); // distinct tuples -> distinct strings
    }
}
