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

package com.questdb.std;

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
                referenceSet.remove(s.toLowerCase());
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
}
