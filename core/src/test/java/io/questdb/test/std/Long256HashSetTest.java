/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.Long256HashSet;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class Long256HashSetTest {

    @Test
    public void testFill() {
        Rnd rnd = new Rnd();

        Long256HashSet set = new Long256HashSet();

        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(set.add(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong()));
        }

        // check that all of that is still in the set
        rnd.reset();

        for (int i = 0; i < 1000; i++) {
            long k0 = rnd.nextLong();
            long k1 = rnd.nextLong();
            long k2 = rnd.nextLong();
            long k3 = rnd.nextLong();
            int index = set.keyIndex(k0, k1, k2, k3);
            Assert.assertTrue(index < 0);
            Assert.assertEquals(k0, set.k0At(index));
            Assert.assertEquals(k1, set.k1At(index));
            Assert.assertEquals(k2, set.k2At(index));
            Assert.assertEquals(k3, set.k3At(index));
        }

        // add same values should fail
        rnd.reset();
        for (int i = 0; i < 1000; i++) {
            Assert.assertFalse(set.add(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong()));
        }

        Long256HashSet set2 = new Long256HashSet(set);

        rnd.reset();
        for (int i = 0; i < 1000; i++) {
            long k0 = rnd.nextLong();
            long k1 = rnd.nextLong();
            long k2 = rnd.nextLong();
            long k3 = rnd.nextLong();
            int index = set2.keyIndex(k0, k1, k2, k3);
            Assert.assertTrue(index < 0);
            Assert.assertEquals(k0, set.k0At(index));
            Assert.assertEquals(k1, set.k1At(index));
            Assert.assertEquals(k2, set.k2At(index));
            Assert.assertEquals(k3, set.k3At(index));
        }
    }
}
