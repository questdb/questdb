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

package com.nfsdb;

import com.nfsdb.storage.HugeBuffer;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HugeBufferTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testIntArrays() throws Exception {
        Rnd rnd = new Rnd();
        HugeBuffer hb = new HugeBuffer(temp.newFile(), 16, JournalMode.APPEND);

        int vals[] = new int[100];

        long pos = hb.getPos();

        for (int i = 0; i < 10000; i++) {
            for (int k = 0; k < vals.length; k++) {
                vals[k] = rnd.nextInt();
            }
            hb.put(vals);
        }

        Rnd expected = new Rnd();

        hb.setPos(pos);

        for (int i = 0; i < 10000; i++) {
            hb.get(vals);
            for (int k = 0; k < vals.length; k++) {
                Assert.assertEquals(expected.nextInt(), vals[k]);
            }
        }
    }

    @Test
    public void testLongArrays() throws Exception {
        Rnd rnd = new Rnd();
        HugeBuffer hb = new HugeBuffer(temp.newFile(), 16, JournalMode.APPEND);

        long vals[] = new long[100];

        long pos = hb.getPos();

        for (int i = 0; i < 10000; i++) {
            for (int k = 0; k < vals.length; k++) {
                vals[k] = rnd.nextLong();
            }
            hb.put(vals);
        }

        Rnd expected = new Rnd();

        hb.setPos(pos);

        for (int i = 0; i < 10000; i++) {
            hb.get(vals);
            for (int k = 0; k < vals.length; k++) {
                Assert.assertEquals(expected.nextLong(), vals[k]);
            }
        }
    }

}
