/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.std;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class CharSequenceHashSetTest {

    @Test
    public void testNullHandling() {
        Rnd rnd = new Rnd();
        CharSequenceHashSet set = new CharSequenceHashSet();
        int n = 1000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd).toString());
        }

        Assert.assertFalse(set.contains(null));
        Assert.assertTrue(set.add(null));
        Assert.assertEquals(n + 1, set.size());
        Assert.assertFalse(set.add(null));
        Assert.assertEquals(n + 1, set.size());
        Assert.assertTrue(set.contains(null));
        Assert.assertTrue(set.remove(null) > -1);
        Assert.assertEquals(n, set.size());
        Assert.assertEquals(set.remove(null), -1);
        Assert.assertEquals(n, set.size());
    }

    @Test
    public void testStress() {
        Rnd rnd = new Rnd();
        CharSequenceHashSet set = new CharSequenceHashSet();
        int n = 10000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd).toString());
        }

        Assert.assertEquals(n, set.size());

        HashSet<String> check = new HashSet<>();
        for (int i = 0, m = set.size(); i < m; i++) {
            check.add(set.get(i).toString());
        }

        Assert.assertEquals(n, check.size());

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

        for (int i = 0; i < n; i++) {
            Assert.assertEquals("at " + i, set.remove(next(rnd)), -1);
        }

        Rnd rnd4 = new Rnd();
        for (int i = 0; i < n; i++) {
            Assert.assertTrue("at " + i, set.remove(next(rnd4)) > -1);
        }

        Assert.assertEquals(0, set.size());
    }

    private static CharSequence next(Rnd rnd) {
        return rnd.nextChars((rnd.nextInt() & 15) + 10);
    }
}