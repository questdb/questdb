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

package io.questdb.test.std;

import io.questdb.std.CompactUtf8SequenceHashSet;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.junit.Assert;
import org.junit.Test;

public class CompactUtf8SequenceHashSetTest {

    @Test
    public void testResetCapacity() {
        Rnd rnd = new Rnd();
        Utf8StringSink sink = new Utf8StringSink();
        CompactUtf8SequenceHashSet set = new CompactUtf8SequenceHashSet();
        int n = 1000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd, sink));
        }

        rnd.reset();
        Assert.assertEquals(n, set.size());
        for (int i = 0; i < n; i++) {
            Assert.assertTrue(set.contains(next(rnd, sink)));
        }

        set.resetCapacity();

        rnd.reset();
        Assert.assertEquals(0, set.size());
        for (int i = 0; i < n; i++) {
            Assert.assertFalse(set.contains(next(rnd, sink)));
        }
    }

    @Test
    public void testStress() {
        Rnd rnd = new Rnd();
        Utf8StringSink sink = new Utf8StringSink();
        CompactUtf8SequenceHashSet set = new CompactUtf8SequenceHashSet();
        int n = 10000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd, sink));
        }

        Assert.assertEquals(n, set.size());

        Rnd rnd2 = new Rnd();
        for (int i = 0; i < n; i++) {
            Assert.assertTrue("at " + i, set.contains(next(rnd2, sink)));
        }

        Assert.assertEquals(n, set.size());

        Rnd rnd3 = new Rnd();
        for (int i = 0; i < n; i++) {
            Assert.assertFalse("at " + i, set.add(next(rnd3, sink)));
        }

        Assert.assertEquals(n, set.size());

        for (int i = 0; i < n; i++) {
            Assert.assertEquals("at " + i, set.remove(next(rnd, sink)), -1);
        }

        Rnd rnd4 = new Rnd();
        for (int i = 0; i < n; i++) {
            Assert.assertTrue("at " + i, set.remove(next(rnd4, sink)) > -1);
        }

        Assert.assertEquals(0, set.size());
    }

    private static Utf8Sequence next(Rnd rnd, Utf8StringSink sink) {
        sink.clear();
        rnd.nextUtf8Str((rnd.nextInt() & 15) + 10, sink);
        return sink;
    }
}
