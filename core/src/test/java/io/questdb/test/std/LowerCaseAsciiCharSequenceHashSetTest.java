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

import io.questdb.std.Chars;
import io.questdb.std.LowerCaseAsciiCharSequenceHashSet;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class LowerCaseAsciiCharSequenceHashSetTest {
    @Test
    public void testSaturation() {

        final int N = 10_000;
        final Rnd rnd = new Rnd();
        final LowerCaseAsciiCharSequenceHashSet lowerCaseSet = new LowerCaseAsciiCharSequenceHashSet();
        final HashSet<String> referenceSet = new HashSet<>();
        for (int i = 0; i < N; i++) {
            String str = rnd.nextString(4);
            int keyIndex = lowerCaseSet.keyIndex(str, 0, str.length());
            if (lowerCaseSet.add(str)) {
                Assert.assertTrue("at " + i, referenceSet.add(str.toLowerCase()));
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

        for (int i = 0, n = lowerCaseSet.getKeyCount(); i < n; i++) {
            CharSequence v = lowerCaseSet.getKey(i);
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
                referenceSet.remove(s.toLowerCase());
            }
        }

        // verify
        for (String s : referenceSet) {
            Assert.assertTrue(s, lowerCaseSet.contains(s));
        }

        for (int i = 0, n = lowerCaseSet.getKeyCount(); i < n; i++) {
            CharSequence v = lowerCaseSet.getKey(i);
            if (v != null) {
                Assert.assertTrue(referenceSet.contains(v.toString()));
            }
        }
    }
}
