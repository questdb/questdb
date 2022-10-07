/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class LowerCaseCharSequenceHashSetTest {
    @Test
    public void testSaturation() {

        final int N = 10_000;
        final Rnd rnd = new Rnd();
        final LowerCaseCharSequenceHashSet lowerCaseSet = new LowerCaseCharSequenceHashSet();
        final HashSet<String> referenceSet = new HashSet<>();
        for (int i = 0; i < N; i++) {
            String str = rnd.nextString(4);
            int keyIndex = lowerCaseSet.keyIndex(str, 0, str.length());
            if (lowerCaseSet.add(str)) {
                Assert.assertTrue("at " + i, referenceSet.add(str));
                Assert.assertTrue(keyIndex > -1);
            } else {
                Assert.assertTrue(keyIndex < 0);
            }
        }

        // verify
        for (String s : referenceSet) {
            Assert.assertTrue(lowerCaseSet.contains(s));
            Assert.assertFalse(lowerCaseSet.excludes(s));
            Assert.assertFalse(lowerCaseSet.excludes(s, 0, s.length()));

            int keyIndex = lowerCaseSet.keyIndex(s);
            Assert.assertTrue(keyIndex < 0);
            Assert.assertTrue(Chars.equals(s, lowerCaseSet.keyAt(keyIndex)));

            keyIndex = lowerCaseSet.keyIndex(s, 0, s.length());
            Assert.assertTrue(Chars.equals(s, lowerCaseSet.keyAt(keyIndex)));
        }

        for (int i = 0, n = lowerCaseSet.keys.length; i < n; i++) {
            CharSequence v = lowerCaseSet.keys[i];
            if (v != null) {
                Assert.assertTrue(referenceSet.contains(v.toString()));
            }
        }

        // remove every forth key
        // make sure rnd generates the same keys again
        rnd.reset();

        for (int i = 0; i < N; i++) {
            String s = rnd.nextString(4);
            if (i % 4 == 0) {
                lowerCaseSet.remove(s);
                referenceSet.remove(s);
            }
        }

        // verify
        for (String s : referenceSet) {
            Assert.assertTrue(s, lowerCaseSet.contains(s));
        }

        for (int i = 0, n = lowerCaseSet.keys.length; i < n; i++) {
            CharSequence v = lowerCaseSet.keys[i];
            if (v != null) {
                Assert.assertTrue(referenceSet.contains(v.toString()));
            }
        }
    }

    @Test
    public void testEqualsAndHashCode() {
        final int items = 1000;

        final LowerCaseCharSequenceHashSet setA = new LowerCaseCharSequenceHashSet();
        final LowerCaseCharSequenceHashSet setB = new LowerCaseCharSequenceHashSet();

        Assert.assertEquals(setA, setB);
        Assert.assertEquals(setA.hashCode(), setB.hashCode());

        for (int i = 0; i < items; i++) {
            setA.add(Integer.toString(i));
        }

        Assert.assertNotEquals(setA, setB);

        // Reverse the addition order, so that the elements of the underlying arrays aren't 1-to-1 between the sets.
        for (int i = items - 1; i > -1; i--) {
            setB.add(Integer.toString(i));
        }

        Assert.assertEquals(setA, setB);
        Assert.assertEquals(setA.hashCode(), setB.hashCode());

        setA.clear();
        setB.clear();

        Assert.assertEquals(setA, setB);
        Assert.assertEquals(setA.hashCode(), setB.hashCode());
    }
}