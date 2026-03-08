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

import io.questdb.std.CompactCharSequenceHashSet;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class CompactCharSequenceHashSetTest {

    @Test
    public void testBasic() {
        Rnd rnd = new Rnd();
        CompactCharSequenceHashSet set = new CompactCharSequenceHashSet();
        int n = 1000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd).toString());
        }

        Assert.assertEquals(n, set.size());
    }

    @Test
    public void testResetCapacity() {
        Rnd rnd = new Rnd();
        CompactCharSequenceHashSet set = new CompactCharSequenceHashSet();
        int n = 1000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd).toString());
        }

        rnd.reset();
        Assert.assertEquals(n, set.size());
        for (int i = 0; i < n; i++) {
            Assert.assertTrue(set.contains(next(rnd).toString()));
        }

        rnd.reset();
        set.resetCapacity();
        Assert.assertEquals(0, set.size());
        for (int i = 0; i < n; i++) {
            Assert.assertFalse(set.contains(next(rnd).toString()));
        }
    }

    @Test
    public void testStress() {
        Rnd rnd = new Rnd();
        CompactCharSequenceHashSet set = new CompactCharSequenceHashSet();
        int n = 10000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd).toString());
        }

        Assert.assertEquals(n, set.size());

        Rnd rnd2 = new Rnd();
        for (int i = 0; i < n; i++) {
            Assert.assertTrue("at " + i, set.contains(next(rnd2)));
        }

        Assert.assertEquals(n, set.size());

        Rnd rnd3 = new Rnd();
        for (int i = 0; i < n; i++) {
            Assert.assertFalse("at " + i, set.add(next(rnd3)));
        }

        Assert.assertEquals(n, set.size());
    }

    private static CharSequence next(Rnd rnd) {
        return rnd.nextChars((rnd.nextInt() & 15) + 10);
    }
}
