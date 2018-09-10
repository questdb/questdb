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

public class CharSequenceIntHashMapTest {

    @Test
    public void testAll() {

        Rnd rnd = new Rnd();
        // populate map
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            boolean b = map.put(cs, rnd.nextInt());
            Assert.assertTrue(b);
        }
        Assert.assertEquals(N, map.size());

        rnd.reset();

        // assert that map contains the values we just added
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            Assert.assertFalse(map.excludes(cs));
            Assert.assertEquals(rnd.nextInt(), map.get(cs));
        }

        Rnd rnd2 = new Rnd();

        rnd.reset();

        // remove some keys and assert that the size() complies
        int removed = 0;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.remove(cs) > -1);
                removed++;
                Assert.assertEquals(N - removed, map.size());
            }
        }

        // if we didn't remove anything test has no value
        Assert.assertTrue(removed > 0);

        rnd2.reset();
        rnd.reset();

        Rnd rnd3 = new Rnd();

        // assert that keys we didn't remove are still there and
        // keys we removed are not
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            int value = rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.excludes(cs));
            } else {
                Assert.assertFalse(map.excludes(cs));

                int index = map.keyIndex(cs);
                Assert.assertEquals(value, map.valueAt(index));

                // update value
                map.putAt(index, cs, rnd3.nextInt());
            }
        }

        rnd.reset();
        rnd2.reset();
        rnd3.reset();

        // increment values
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 != 0) {
                map.increment(cs);
                int index = map.keyIndex(cs);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(rnd3.nextInt() + 1, map.valueAt(index));
            } else {
                map.increment(cs);
                int index = map.keyIndex(cs);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(0, map.valueAt(index));
            }
        }

        // put all values into another map

        CharSequenceIntHashMap map2 = new CharSequenceIntHashMap();
        map2.putAll(map);

        // assert values
        rnd.reset();
        rnd2.reset();
        rnd3.reset();
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 != 0) {
                int index = map2.keyIndex(cs);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(rnd3.nextInt() + 1, map2.valueAt(index));
            } else {
                int index = map2.keyIndex(cs);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(0, map2.valueAt(index));
            }
        }

        // re-populate map
        rnd.reset();
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            map.put(cs, rnd.nextInt());
        }

        // remove some keys again
        rnd.reset();
        rnd2.reset();
        removed = 0;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            rnd.nextInt();
            if (rnd2.nextPositiveInt() % 4 == 0) {
                map.remove(cs);
                removed++;
                Assert.assertEquals(N - removed, map.size());
            }
        }

        // assert
        rnd.reset();
        rnd2.reset();
        rnd3.reset();

        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            rnd.nextInt();
            map.putIfAbsent(cs, rnd3.nextInt());
        }

        // assert
        rnd.reset();
        rnd2.reset();
        rnd3.reset();
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            int value1 = rnd.nextInt();
            int value2 = rnd3.nextInt();
            if (rnd2.nextPositiveInt() % 4 == 0) {
                Assert.assertEquals(value2, map.get(cs));
            } else {
                Assert.assertEquals(value1, map.get(cs));
            }
        }
    }

    @Test
    public void testPartialLookup() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        Rnd rnd = new Rnd();
        final int N = 1000;

        for (int i = 0; i < N; i++) {
            String s = rnd.nextString(10).substring(1, 9);
            map.put(s, i);
        }

        rnd.reset();

        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextString(10);
            int index = map.keyIndex(cs, 1, 9);
            Assert.assertEquals(i, map.valueAt(index));
        }

        rnd.reset();
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextString(10);
            Assert.assertFalse(map.excludes(cs, 1, 9));
        }
    }


}