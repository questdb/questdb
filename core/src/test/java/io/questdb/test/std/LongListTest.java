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

import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.LongSort;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LongListTest {
    @Test
    public void testAddAll_CapacityExpansion() {
        LongList dst = new LongList(2);
        dst.add(1L);
        dst.add(2L);

        // Add 3 more elements through addAll, forcing capacity expansion
        LongList src = new LongList();
        src.add(3L);
        src.add(4L);
        src.add(5L);

        dst.addAll(src);

        Assert.assertEquals(5, dst.size());
        Assert.assertEquals(1L, dst.getQuick(0));
        Assert.assertEquals(2L, dst.getQuick(1));
        Assert.assertEquals(3L, dst.getQuick(2));
        Assert.assertEquals(4L, dst.getQuick(3));
        Assert.assertEquals(5L, dst.getQuick(4));
    }

    @Test
    public void testAddAll_EmptyToEmpty() {
        LongList dst = new LongList();
        LongList src = new LongList();
        dst.addAll(src);

        Assert.assertEquals(0, dst.size());
    }

    @Test
    public void testAddAll_EmptyToNonEmpty() {
        LongList dst = new LongList();
        dst.add(1L);
        dst.add(2L);

        LongList src = new LongList();
        dst.addAll(src);

        Assert.assertEquals(2, dst.size());
        Assert.assertEquals(1L, dst.getQuick(0));
        Assert.assertEquals(2L, dst.getQuick(1));
    }

    @Test
    public void testAddAll_NonEmptyToEmpty() {
        LongList dst = new LongList();
        LongList src = new LongList();
        src.add(1L);
        src.add(2L);
        src.add(3L);

        dst.addAll(src);

        Assert.assertEquals(3, dst.size());
        Assert.assertEquals(1L, dst.getQuick(0));
        Assert.assertEquals(2L, dst.getQuick(1));
        Assert.assertEquals(3L, dst.getQuick(2));
    }

    @Test
    public void testAddAll_NonEmptyToNonEmpty() {
        LongList dst = new LongList();
        dst.add(1L);
        dst.add(2L);

        LongList src = new LongList();
        src.add(3L);
        src.add(4L);

        dst.addAll(src);

        Assert.assertEquals(4, dst.size());
        Assert.assertEquals(1L, dst.getQuick(0));
        Assert.assertEquals(2L, dst.getQuick(1));
        Assert.assertEquals(3L, dst.getQuick(2));
        Assert.assertEquals(4L, dst.getQuick(3));
    }

    @Test
    public void testAddAll_PreservesSourceList() {
        LongList dst = new LongList();
        LongList src = new LongList();
        src.add(1L);
        src.add(2L);

        dst.addAll(src);
        src.add(3L); // Modify source after addAll

        // Verify destination list wasn't affected by source modification
        Assert.assertEquals(2, dst.size());
        Assert.assertEquals(1L, dst.getQuick(0));
        Assert.assertEquals(2L, dst.getQuick(1));

        // Verify source list is intact
        Assert.assertEquals(3, src.size());
        Assert.assertEquals(1L, src.getQuick(0));
        Assert.assertEquals(2L, src.getQuick(1));
        Assert.assertEquals(3L, src.getQuick(2));
    }

    @Test
    public void testBinarySearchBlockFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int N = 997; // prime
        final int skipRate = 1 + rnd.nextInt(3);
        final int dupeRate = 1 + rnd.nextInt(7);
        final int dupeCountBound = 4;

        for (int c = 0; c < N; c++) {
            for (int k = 1; k < dupeCountBound; k++) {
                testBinarySearchBlockFuzz0(c, skipRate, dupeRate, k);
            }
        }

        testBinarySearchBlockFuzz0(1, 0, 1, 1024);
    }

    @Test
    public void testBinarySearchFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int N = 997; // prime
        final int skipRate = 1 + rnd.nextInt(3);
        final int dupeRate = 1 + rnd.nextInt(7);
        final int dupeCountBound = 4;

        for (int c = 0; c < N; c++) {
            for (int k = 1; k < dupeCountBound; k++) {
                testBinarySearchFuzz0(c, skipRate, dupeRate, k);
            }
        }

        testBinarySearchFuzz0(1, 0, 1, 1024);
    }

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
        Assert.assertNotEquals(null, list1);

        // different noEntryValue
        final LongList list6 = new LongList(4, Long.MIN_VALUE);
        list6.add(1L);
        list6.add(2L);
        list6.add(3L);
        Assert.assertNotEquals(list1, list6);

        // equals
        final LongList list7 = new LongList();
        list7.add(1L);
        list7.add(2L);
        list7.add(3L);
        Assert.assertEquals(list1, list7);

        final LongList list8 = new LongList(4, -1L);
        list8.add(1L);
        list8.add(2L);
        list8.add(3L);
        Assert.assertEquals(list1, list8);
    }

    @Test
    public void testIndexOf() {
        LongList list = new LongList();
        for (int i = 100; i > -1; i--) {
            list.add(i);
        }

        for (int i = 100; i > -1; i--) {
            Assert.assertEquals(100 - i, list.indexOf(i));
        }
    }

    @Test
    public void testInsertFromSource() {
        LongList src = new LongList();
        src.add(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        LongList dst = new LongList();
        dst.insertFromSource(0, src, 0, src.size());
        TestUtils.assertEquals(src, dst);

        dst.clear();
        dst.add(-1L, -2L, -3L, -4L);
        dst.insertFromSource(4, src, 4, src.size());
        LongList expected = new LongList();
        expected.add(-1L, -2L, -3L, -4L, 5L, 6L, 7L, 8L);
        Assert.assertEquals(expected, dst);
    }

    @Test
    public void testMergeSortStructuredLargeList() {
        long[] data = new long[]{1375128000000000L, 1375131600000000L, 1375135200000000L, 1375138800000000L, 1375142400000000L, 1375146000000000L, 1375149600000000L, 1375153200000000L, 1375156800000000L, 1375160400000000L, 1375164000000000L, 1375167600000000L, 1375171200000000L, 1375174800000000L, 1375178400000000L, 1375182000000000L, 1373317200000000L, 1373313600000000L, 1373324400000000L, 1375196400000000L, 1375200000000000L, 1375203600000000L, 1375207200000000L, 1375210800000000L, 1375214400000000L, 1375218000000000L, 1375221600000000L, 1375225200000000L, 1373385600000000L, 1373389200000000L, 1373392800000000L, 1375236000000000L, 1373400000000000L, 1373403600000000L, 1373407200000000L, 1373410800000000L, 1373414400000000L, 1373418000000000L, 1373421600000000L, 1373425200000000L, 1373428800000000L, 1373432400000000L, 1373436000000000L, 1373439600000000L, 1373443200000000L, 1373446800000000L, 1373450400000000L, 1373454000000000L, 1373457600000000L, 1373461200000000L, 1373464800000000L, 1373468400000000L, 1373472000000000L, 1373475600000000L, 1373479200000000L, 1373482800000000L, 1375246800000000L, 1375290000000000L, 1375279200000000L, 1375275600000000L, 1375282800000000L, 1375254000000000L, 1375250400000000L, 1375239600000000L, 1375268400000000L, 1375232400000000L, 1375243200000000L, 1375293600000000L, 1375272000000000L, 1375257600000000L, 1375297200000000L, 1375286400000000L, 1375261200000000L, 1375264800000000L, 1375228800000000L, 1373360400000000L, 1373364000000000L, 1375192800000000L, 1373320800000000L, 1373832000000000L, 1373835600000000L, 1373839200000000L, 1373842800000000L, 1373846400000000L, 1373850000000000L, 1373853600000000L, 1373857200000000L, 1373860800000000L, 1373864400000000L, 1373868000000000L, 1373871600000000L, 1373875200000000L, 1373878800000000L, 1373882400000000L, 1373886000000000L, 1373889600000000L, 1373893200000000L, 1373896800000000L, 1373900400000000L, 1373904000000000L, 1373907600000000L, 1373911200000000L, 1373914800000000L, 1374436800000000L,
                1374440400000000L, 1374444000000000L, 1374447600000000L, 1374451200000000L, 1374454800000000L, 1374458400000000L, 1374462000000000L, 1374465600000000L, 1374469200000000L, 1374472800000000L, 1374476400000000L, 1374480000000000L, 1374483600000000L, 1374487200000000L, 1374490800000000L, 1374494400000000L, 1374498000000000L, 1374501600000000L, 1374505200000000L, 1374508800000000L, 1374512400000000L, 1374516000000000L, 1374519600000000L, 1374523200000000L, 1374526800000000L, 1374530400000000L, 1374534000000000L, 1374537600000000L, 1374541200000000L, 1374544800000000L, 1374548400000000L, 1372708800000000L, 1372712400000000L, 1372716000000000L, 1372719600000000L, 1374566400000000L, 1374570000000000L, 1372730400000000L, 1374577200000000L, 1372737600000000L, 1372741200000000L, 1372744800000000L, 1372748400000000L, 1372752000000000L, 1374598800000000L, 1372759200000000L, 1372762800000000L, 1372766400000000L, 1372770000000000L, 1372773600000000L, 1372777200000000L, 1372780800000000L, 1372784400000000L, 1372788000000000L, 1372791600000000L, 1372795200000000L, 1372798800000000L, 1372802400000000L, 1372806000000000L, 1372809600000000L, 1372813200000000L, 1372816800000000L, 1372820400000000L, 1372824000000000L, 1372827600000000L, 1372831200000000L, 1372834800000000L, 1372838400000000L, 1372842000000000L, 1372845600000000L, 1372849200000000L, 1372852800000000L, 1372856400000000L, 1372860000000000L, 1372863600000000L, 1372867200000000L, 1372870800000000L, 1372874400000000L, 1372878000000000L, 1372755600000000L, 1372734000000000L, 1372726800000000L, 1372723200000000L, 1374602400000000L,
                1374562800000000L, 1374580800000000L, 1374588000000000L, 1374552000000000L, 1374584400000000L, 1374559200000000L, 1374573600000000L, 1374555600000000L, 1374595200000000L, 1374591600000000L, 1374606000000000L, 1375027200000000L, 1375030800000000L, 1375034400000000L, 1375038000000000L, 1375041600000000L, 1375045200000000L, 1375048800000000L, 1375052400000000L, 1375056000000000L, 1375059600000000L, 1375063200000000L, 1375066800000000L, 1375070400000000L, 1375074000000000L, 1375077600000000L, 1375081200000000L, 1375084800000000L, 1375088400000000L, 1375092000000000L, 1375095600000000L, 1375099200000000L, 1375102800000000L, 1375106400000000L, 1375110000000000L, 1375113600000000L, 1375117200000000L, 1375120800000000L, 1375124400000000L, 1375185600000000L, 1375189200000000L, 1373328000000000L, 1373331600000000L, 1373335200000000L, 1373338800000000L, 1373342400000000L, 1373346000000000L, 1373349600000000L, 1373353200000000L, 1373356800000000L, 1373367600000000L, 1373371200000000L, 1373374800000000L, 1373378400000000L, 1373382000000000L, 1373396400000000L, 1374264000000000L, 1374267600000000L, 1374271200000000L, 1374274800000000L, 1374278400000000L, 1374282000000000L, 1374285600000000L, 1374289200000000L, 1374292800000000L, 1374296400000000L, 1374300000000000L, 1374303600000000L, 1374307200000000L, 1374310800000000L, 1374314400000000L, 1374318000000000L, 1374321600000000L, 1374325200000000L, 1374328800000000L, 1374332400000000L, 1374336000000000L, 1374339600000000L, 1374343200000000L, 1374346800000000L, 1374350400000000L, 1374354000000000L, 1374357600000000L, 1374361200000000L, 1374364800000000L, 1374368400000000L, 1374372000000000L, 1374375600000000L, 1374379200000000L, 1374382800000000L, 1374386400000000L, 1374390000000000L, 1374393600000000L, 1374397200000000L, 1374400800000000L, 1374404400000000L, 1374408000000000L, 1374411600000000L, 1374415200000000L, 1374418800000000L, 1374422400000000L, 1374426000000000L, 1374429600000000L, 1374433200000000L, 1374955200000000L,
                1374958800000000L, 1374962400000000L, 1374966000000000L, 1374969600000000L, 1374973200000000L, 1374976800000000L, 1374980400000000L, 1374984000000000L, 1374987600000000L, 1374991200000000L, 1374994800000000L, 1374998400000000L, 1375002000000000L, 1375005600000000L, 1375009200000000L, 1375012800000000L, 1375016400000000L, 1375020000000000L, 1375023600000000L, 1372968000000000L, 1372971600000000L, 1372975200000000L, 1372978800000000L, 1372982400000000L, 1372986000000000L, 1372989600000000L, 1372993200000000L, 1372996800000000L, 1373000400000000L, 1373004000000000L, 1373007600000000L, 1373011200000000L, 1373014800000000L, 1373018400000000L, 1373022000000000L, 1373025600000000L, 1373029200000000L, 1373032800000000L, 1373036400000000L, 1373040000000000L, 1373043600000000L, 1373047200000000L, 1373050800000000L, 1373054400000000L, 1373058000000000L, 1373061600000000L, 1373065200000000L, 1373068800000000L, 1373072400000000L, 1373076000000000L, 1373079600000000L, 1373083200000000L, 1373086800000000L, 1373090400000000L, 1373094000000000L, 1373097600000000L, 1373101200000000L, 1373104800000000L, 1373108400000000L, 1373112000000000L, 1373115600000000L, 1373119200000000L, 1373122800000000L, 1373126400000000L, 1373130000000000L, 1373133600000000L, 1373137200000000L, 1373140800000000L, 1373144400000000L, 1373148000000000L, 1373151600000000L, 1373155200000000L, 1373158800000000L, 1373162400000000L, 1373166000000000L, 1373169600000000L, 1373173200000000L, 1373176800000000L, 1373180400000000L, 1373184000000000L, 1373187600000000L, 1373191200000000L, 1373194800000000L, 1373198400000000L, 1373202000000000L, 1373205600000000L, 1373209200000000L, 1373212800000000L, 1373216400000000L, 1373220000000000L, 1373223600000000L, 1373227200000000L, 1373230800000000L, 1373234400000000L, 1373238000000000L, 1373241600000000L, 1373245200000000L, 1373248800000000L, 1373252400000000L, 1373256000000000L, 1373259600000000L, 1373263200000000L, 1373266800000000L, 1373270400000000L, 1373274000000000L,
                1373277600000000L, 1373281200000000L, 1373284800000000L, 1373288400000000L, 1373292000000000L, 1373295600000000L, 1373299200000000L, 1373302800000000L, 1373306400000000L, 1373310000000000L};
        LongList list = new LongList(data);

        list.sort();

        assertOrderedAsc(list);
    }

    @Test
    public void testMergeSortStructuredLargeList2() {
        int blockSize = LongSort.MAX_RUN_COUNT / 2;
        long blockStartValue = 0;

        LongList list = new LongList();
        for (int l = 0; l < LongSort.QUICKSORT_THRESHOLD + 10; l++) {
            int blockIdx = l % blockSize;
            if (blockIdx == 0) {
                blockStartValue = -l;
            }
            list.add(blockStartValue + l);
        }

        list.sort();

        assertOrderedAsc(list);
    }

    @Test
    public void testMergeSortStructuredSmallList() {
        LongList list = new LongList();
        for (int l = 0; l < LongSort.INSERTION_SORT_THRESHOLD - 1; l++) {
            list.add(l);
        }

        list.sort();

        assertOrderedAsc(list);
    }

    @Test
    public void testMergeSortStructuredSmallListAllEqual() {
        LongList list = new LongList();
        for (int l = 0; l < LongSort.INSERTION_SORT_THRESHOLD - 1; l++) {
            list.add(42);
        }

        list.sort();

        assertOrderedAsc(list);
    }

    @Test
    public void testRestoreInitialCapacity() {
        final int N = 1000;
        LongList list = new LongList();
        int initialCapacity = list.capacity();

        for (int i = 0; i < N; i++) {
            list.add(i);
        }

        Assert.assertEquals(N, list.size());
        Assert.assertTrue(list.capacity() >= N);

        list.restoreInitialCapacity();
        Assert.assertEquals(0, list.size());
        Assert.assertEquals(initialCapacity, list.capacity());
    }

    @Test
    public void testSmoke() {
        LongList list = new LongList();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        Assert.assertEquals(100, list.size());
        Assert.assertTrue(list.capacity() >= 100);

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, list.getQuick(i));
            Assert.assertEquals(i, list.indexOf(i));
        }

        for (int i = 0; i < 100; i++) {
            list.remove(i);
        }
        Assert.assertEquals(0, list.size());
    }

    private void assertOrderedAsc(LongList list) {
        for (int i = 0, n = list.size() - 1; i < n; i++) {
            try {
                Assert.assertTrue(list.getQuick(i) <= list.getQuick(i + 1));
            } catch (AssertionError ae) {
                long v1 = list.getQuick(i);
                long v2 = list.getQuick(i + 1);
                throw new AssertionError("List is not sorted at position=" + i + " value(i)=" + v1 + ",value(i+1)=" + v2 + ",diff=" + (v2 - v1));
            }
        }
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
            int pos = list.binarySearchBlock(2, i, Vect.BIN_SEARCH_SCAN_UP);
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
            int pos = list.binarySearchBlock(2, i, Vect.BIN_SEARCH_SCAN_DOWN);
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

        int pos = list.binarySearch(N, Vect.BIN_SEARCH_SCAN_UP);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(pos, list.size());

        // search min value (less than anything in the list)

        pos = list.binarySearch(-1, Vect.BIN_SEARCH_SCAN_UP);
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
            int pos = list.binarySearch(i, Vect.BIN_SEARCH_SCAN_UP);
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
            int pos = list.binarySearch(i, Vect.BIN_SEARCH_SCAN_DOWN);
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

        int pos = list.binarySearch(N, Vect.BIN_SEARCH_SCAN_UP);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(pos, list.size());

        // search min value (less than anything in the list)

        pos = list.binarySearch(-1, Vect.BIN_SEARCH_SCAN_UP);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(0, pos);
    }
}
