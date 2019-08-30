/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

public class IntLongAssociativeCacheTest {

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
//        Assert.assertEquals(512, reject.size());
    }


}