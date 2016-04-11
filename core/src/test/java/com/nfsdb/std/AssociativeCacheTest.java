/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.std;

import com.nfsdb.misc.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class AssociativeCacheTest {
    @Test
    public void testBasic() throws Exception {
        AssociativeCache<String> cache = new AssociativeCache<>(8, 64);
        cache.put("X", "1");
        cache.put("Y", "2");
        cache.put("Z", "3");
        Assert.assertEquals("1", cache.peek("X"));
        Assert.assertEquals("2", cache.peek("Y"));
        Assert.assertEquals("3", cache.peek("Z"));
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
            if (cache.peek(k) == null) {
                Assert.assertTrue(reject.contains(k));
            }
        }
        Assert.assertEquals(512, reject.size());
    }
}
