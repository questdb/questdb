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

package io.questdb.test.std;

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class CharSequenceHashSetTest {

    @Test
    public void testNullBeforeRehash() {
        for (int nullPosition : new int[]{0, 15, 16}) {
            for (int mode = 0; mode < 2; mode++) {
                CharSequenceHashSet set = new CharSequenceHashSet();
                ObjList<CharSequence> expected = new ObjList<>();
                for (int i = 0; i < 80; i++) {
                    if (i == nullPosition) {
                        Assert.assertTrue(set.addNull());
                        Assert.assertFalse(set.addNull());
                        expected.add(null);
                    }
                    StringSink key = new StringSink();
                    key.put("key_").put(i);
                    if (mode == 0) {
                        Assert.assertTrue(set.add(key));
                        expected.add(key.toString());
                    } else {
                        set.addAtWithBorrowed(set.keyIndex(key), key);
                        Assert.assertSame(key, set.keyAt(set.keyIndex(key)));
                        expected.add(key);
                    }
                }
                assertContents(set, expected);
                Assert.assertEquals(nullPosition, set.getList().indexOf(null));
                CharSequenceHashSet copy = new CharSequenceHashSet(set);
                CharSequenceHashSet added = new CharSequenceHashSet();
                added.addAll(set);
                assertContents(copy, expected);
                assertContents(added, expected);
                Assert.assertTrue(set.remove("key_7") >= 0);
                expected.remove(expected.getQuick(7 < nullPosition ? 7 : 8));
                Assert.assertEquals(nullPosition > 7 ? nullPosition - 1 : nullPosition, set.removeNull());
                expected.remove(null);
                Assert.assertFalse(set.contains(null));
                Assert.assertFalse(set.contains("key_7"));
                assertContents(set, expected);
                set.clear();
                Assert.assertEquals(0, set.size());
                Assert.assertFalse(set.contains(null));
                set.addAll(copy);
                assertContents(set, copy.getList());
                Assert.assertEquals(81, set.size());
                Assert.assertTrue(set.contains(null));
                Assert.assertTrue(set.contains("key_7"));
                Assert.assertEquals(nullPosition, set.getList().indexOf(null));
            }
        }
    }

    @Test
    public void testNullHandling() {
        Rnd rnd = new Rnd();
        CharSequenceHashSet set = new CharSequenceHashSet();
        int n = 1000;

        for (int i = 0; i < n; i++) {
            set.add(next(rnd).toString());
        }

        Assert.assertFalse(set.contains(null));
        Assert.assertTrue(set.add((CharSequence) null));
        Assert.assertEquals(n + 1, set.size());
        Assert.assertFalse(set.add((CharSequence) null));
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

    private static void assertContents(CharSequenceHashSet set, ObjList<CharSequence> expected) {
        Assert.assertEquals(expected.size(), set.size());
        for (int i = 0; i < expected.size(); i++) {
            CharSequence key = expected.getQuick(i);
            Assert.assertTrue(set.contains(key));
            Assert.assertEquals(key == null ? null : key.toString(), set.get(i) == null ? null : set.get(i).toString());
        }
    }

    private static CharSequence next(Rnd rnd) {
        return rnd.nextChars((rnd.nextInt() & 15) + 10);
    }
}