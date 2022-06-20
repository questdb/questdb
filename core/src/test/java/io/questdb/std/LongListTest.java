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

import io.questdb.cairo.BinarySearch;
import org.junit.Assert;
import org.junit.Test;

public class LongListTest {
    @Test
    public void testEquals() {
        final LongList list1 = new LongList();
        list1.add(1L);
        list1.add(2L);
        list1.add(3L);

        // different order
        final LongList list2 = new LongList();
        list2.add(1L);
        list2.add(3L);
        list2.add(2L);
        Assert.assertNotEquals(list1, list2);

        // longer
        final LongList list3 = new LongList();
        list3.add(1L);
        list3.add(2L);
        list3.add(3L);
        list3.add(4L);
        Assert.assertNotEquals(list1, list3);

        // shorter
        final LongList list4 = new LongList();
        list4.add(1L);
        list4.add(2L);
        Assert.assertNotEquals(list1, list4);

        // empty
        final LongList list5 = new LongList();
        Assert.assertNotEquals(list1, list5);

        // null
        Assert.assertNotEquals(list1, null);

        // equals
        final LongList list6 = new LongList();
        list6.add(1L);
        list6.add(2L);
        list6.add(3L);
        Assert.assertEquals(list1, list6);
    }

    @Test
    public void testBinarySearchBlockFuzz() {
        final int N = 997; // prime
        final int skipRate = 4;
        final int dupeRate = 8;
        final int dupeCountBound = 4;
        for (int c = 0; c < N; c++) {
            for (int i = 0; i < skipRate; i++) {
                for (int j = 0; j < dupeRate; j++) {
                    for (int k = 1; k < dupeCountBound; k++) {
                        testBinarySearchBlockFuzz0(c, i, j, k);
                    }
                }
            }
        }

        testBinarySearchBlockFuzz0(1, 0, 1, 1024);
    }

    @Test
    public void testBinarySearchFuzz() {
        final int N = 997; // prime
        final int skipRate = 4;
        final int dupeRate = 8;
        final int dupeCountBound = 4;
        for (int c = 0; c < N; c++) {
            for (int i = 0; i < skipRate; i++) {
                for (int j = 0; j < dupeRate; j++) {
                    for (int k = 1; k < dupeCountBound; k++) {
                        testBinarySearchFuzz0(c, i, j, k);
                    }
                }
            }
        }

        testBinarySearchFuzz0(1, 0, 1, 1024);
    }

    private void testBinarySearchBlockFuzz0(int N, int skipRate, int dupeRate, int dupeCountBound) {
        final Rnd rnd = new Rnd();
        final LongList list = new LongList();
        final IntList skipList = new IntList();
        for (int i = 0; i < N; i++) {
            // not skipping ?
            if (skipRate == 0 || rnd.nextInt(skipRate) != 0) {
                list.add(i, 0, 0, 0);
                skipList.add(1);

                boolean dupe = dupeRate > 0 && rnd.nextInt(dupeRate) == 0;
                // duplicating value ?
                if (dupe) {
                    int dupeCount = Math.abs(rnd.nextInt(dupeCountBound));
                    while (dupeCount-- > 0) {
                        list.add(i, 0, 0, 0);
                    }
                }
            } else {
                skipList.add(0);
            }
        }

        // test scan UP
        final int M = list.size();

        for (int i = 0; i < N; i++) {
            int pos = list.binarySearchBlock(2, i, BinarySearch.SCAN_UP);
            int skip = skipList.getQuick(i);

            // the value was skipped
            if (skip == 0) {
                Assert.assertTrue(pos < 0);

                pos = -pos - 1;
                if (pos > 4) {
                    Assert.assertTrue(list.getQuick(pos - 4) < i);
                }

                if (pos < M) {
                    Assert.assertTrue(list.getQuick(pos) > i);
                }
            } else {
                Assert.assertTrue(pos > -1);
                if (pos > 4) {
                    Assert.assertTrue(list.getQuick(pos - 4) < i);
                }
                Assert.assertEquals(list.getQuick(pos), i);
            }
        }

        for (int i = 0; i < N; i++) {
            int pos = list.binarySearchBlock(2, i, BinarySearch.SCAN_DOWN);
            int skip = skipList.getQuick(i);

            // the value was skipped
            if (skip == 0) {
                Assert.assertTrue(pos < 0);

                pos = -pos - 1;

                if (pos > 4) {
                    Assert.assertTrue(list.getQuick(pos - 5) < i);
                }

                if (pos < M) {
                    Assert.assertTrue(list.getQuick(pos) > i);
                }
            } else {
                Assert.assertTrue(pos > -1);
                Assert.assertEquals(list.getQuick(pos), i);
                if (pos + 4 < M) {
                    // this is a block of 4 longs
                    Assert.assertTrue(list.getQuick(pos + 4) > i);
                }
            }
        }

        // search max value (greater than anything in the list)

        int pos = list.binarySearch(N, BinarySearch.SCAN_UP);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(pos, list.size());

        // search min value (less than anything in the list)

        pos = list.binarySearch(-1, BinarySearch.SCAN_UP);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(0, pos);
    }

    private void testBinarySearchFuzz0(int N, int skipRate, int dupeRate, int dupeCountBound) {
        final Rnd rnd = new Rnd();
        final LongList list = new LongList();
        final IntList skipList = new IntList();
        for (int i = 0; i < N; i++) {
            // not skipping ?
            if (skipRate == 0 || rnd.nextInt(skipRate) != 0) {
                list.add(i);
                skipList.add(1);

                boolean dupe = dupeRate > 0 && rnd.nextInt(dupeRate) == 0;
                // duplicating value ?
                if (dupe) {
                    int dupeCount = Math.abs(rnd.nextInt(dupeCountBound));
                    while (dupeCount-- > 0) {
                        list.add(i);
                    }
                }
            } else {
                skipList.add(0);
            }
        }

        // test scan UP
        final int M = list.size();

        for (int i = 0; i < N; i++) {
            int pos = list.binarySearch(i, BinarySearch.SCAN_UP);
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

        for (int i = 0; i < N; i++) {
            int pos = list.binarySearch(i, BinarySearch.SCAN_DOWN);
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
                Assert.assertEquals(list.getQuick(pos), i);
                if (pos + 1 < M) {
                    Assert.assertTrue(list.getQuick(pos + 1) > i);
                }
            }
        }

        // search max value (greater than anything in the list)

        int pos = list.binarySearch(N, BinarySearch.SCAN_UP);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(pos, list.size());

        // search min value (less than anything in the list)

        pos = list.binarySearch(-1, BinarySearch.SCAN_UP);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(0, pos);
    }
}
