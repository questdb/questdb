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

public class IntListTest {
    @Test
    public void testBinarySearchFuzz() {
        final int N = 997; // prime
        final int skipRate = 4;
        for (int c = 0; c < N; c++) {
            for (int i = 0; i < skipRate; i++) {
                testBinarySearchFuzz0(c, i);
            }
        }

        testBinarySearchFuzz0(1, 0);
    }

    private void testBinarySearchFuzz0(int N, int skipRate) {
        final Rnd rnd = new Rnd();
        final IntList list = new IntList();
        final IntList skipList = new IntList();
        for (int i = 0; i < N; i++) {
            // not skipping ?
            if (skipRate == 0 || rnd.nextInt(skipRate) != 0) {
                list.add(i);
                skipList.add(1);
            } else {
                skipList.add(0);
            }
        }

        // test scan UP
        final int M = list.size();

        for (int i = 0; i < N; i++) {
            int pos = list.binarySearchUniqueList(i);
            int skip = skipList.getQuick(i);

            // the value was skipped
            if (skip == 0) {
                Assert.assertTrue(pos < 0);

                pos = -pos - 1;
                if (pos > 0) {
                    Assert.assertTrue(list.getQuick(pos - 1) < i);
                }

                if (pos < M) {
                    Assert.assertTrue(list.getQuick(pos) > i);
                }
            } else {
                Assert.assertTrue(pos > -1);
                if (pos > 0) {
                    Assert.assertTrue(list.getQuick(pos - 1) < i);
                }
                Assert.assertEquals(list.getQuick(pos), i);
            }
        }


        // search max value (greater than anything in the list)

        int pos = list.binarySearchUniqueList(N);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(pos, list.size());

        // search min value (less than anything in the list)

        pos = list.binarySearchUniqueList(-1);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(0, pos);
    }
}
