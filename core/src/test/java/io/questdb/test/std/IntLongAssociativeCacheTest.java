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

import io.questdb.std.IntHashSet;
import io.questdb.std.IntLongAssociativeCache;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class IntLongAssociativeCacheTest {

    @Test
    public void testBasic() {
        IntLongAssociativeCache cache = new IntLongAssociativeCache(8, 64);
        cache.put(1, 11);
        cache.put(2, 22);
        cache.put(3, 33);

        Assert.assertEquals(11, cache.peek(1));
        Assert.assertEquals(22, cache.peek(2));
        Assert.assertEquals(33, cache.peek(3));

        Assert.assertEquals(11, cache.poll(1));
        Assert.assertEquals(IntLongAssociativeCache.NO_VALUE, cache.poll(1));
        Assert.assertEquals(22, cache.poll(2));
        Assert.assertEquals(IntLongAssociativeCache.NO_VALUE, cache.poll(2));
        Assert.assertEquals(33, cache.poll(3));
        Assert.assertEquals(IntLongAssociativeCache.NO_VALUE, cache.poll(3));
    }

    @Test
    public void testFull() {
        IntLongAssociativeCache cache = new IntLongAssociativeCache(8, 64);
        IntHashSet all = new IntHashSet();
        IntHashSet reject = new IntHashSet();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 16 * 64; i++) {
            int k = rnd.nextPositiveInt() + 1;
            all.add(k);
            int o = cache.put(k, rnd.nextLong());
            if (o != IntLongAssociativeCache.UNUSED_KEY) {
                reject.add(o);
            }
        }

        for (int i = 0; i < all.size(); i++) {
            int k = all.get(i);
            if (cache.peek(k) == IntLongAssociativeCache.NO_VALUE) {
                Assert.assertTrue(reject.contains(k));
            }
        }
    }

    @Test
    public void testMinSize() {
        IntLongAssociativeCache cache = new IntLongAssociativeCache(1, 1);
        cache.put(1, 11);
        cache.put(2, 22);
        cache.put(3, 33);
        Assert.assertEquals(IntLongAssociativeCache.NO_VALUE, cache.peek(1));
        Assert.assertEquals(IntLongAssociativeCache.NO_VALUE, cache.peek(2));
        Assert.assertEquals(33, cache.peek(3));
    }
}