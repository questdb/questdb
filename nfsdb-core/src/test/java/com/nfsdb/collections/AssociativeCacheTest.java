/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 */

package com.nfsdb.collections;

import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class AssociativeCacheTest {
    @Test
    public void testBasic() throws Exception {
        AssociativeCache<String> cache = new AssociativeCache<>(8, 64);
        cache.put("X", "1");
        cache.put("Y", "2");
        cache.put("Z", "3");
        System.out.println(cache.get("X"));
        System.out.println(cache.get("Y"));
        System.out.println(cache.get("Z"));
    }

    @Test
    public void testFull() throws Exception {
        AssociativeCache<String> cache = new AssociativeCache<>(8, 64);
        CharSequenceHashSet all = new CharSequenceHashSet();
        CharSequenceHashSet reject = new CharSequenceHashSet();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 16 * 64; i++) {
            CharSequence k = rnd.nextString(10);
            all.add(k);
            CharSequence o = cache.put(k, rnd.nextString(10));
            if (o != null) {
                reject.add(o);
            }
        }

        for (int i = 0; i < all.size(); i++) {
            CharSequence k = all.get(i);
            if (cache.get(k) == null) {
                Assert.assertTrue(reject.contains(k));
            }
        }
        Assert.assertEquals(512, reject.size());
    }
}
