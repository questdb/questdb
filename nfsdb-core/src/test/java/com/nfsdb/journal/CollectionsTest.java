/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal;

import com.nfsdb.journal.collections.DirectIntList;
import com.nfsdb.journal.collections.DirectLongList;
import org.junit.Assert;
import org.junit.Test;

public class CollectionsTest {

    @Test
    public void testDirectIntList() throws Exception {
        DirectIntList list = new DirectIntList();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            list.add(N - i);
        }

        Assert.assertEquals(N, list.size());

        for (int i = 0; i < N; i++) {
            Assert.assertEquals(N - i, list.get(i));
        }

        // add small list that would not need resizing of target
        DirectIntList list2 = new DirectIntList();
        list2.add(1001);
        list2.add(2001);

        list.add(list2);


        Assert.assertEquals(N + 2, list.size());
        Assert.assertEquals(1001, list.get(N));
        Assert.assertEquals(2001, list.get(N + 1));


        DirectIntList list3 = new DirectIntList();
        for (int i = 0; i < N; i++) {
            list3.add(i + 5000);
        }

        list.add(list3);
        Assert.assertEquals(2 * N + 2, list.size());
    }

    @Test
    public void testMemoryLeak() throws Exception {
        for (int i = 0; i < 10000; i++) {
            new DirectIntList(1000000);
        }
    }

    @Test
    public void testDirectLongList() throws Exception {
        DirectLongList list = new DirectLongList();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            list.add(N - i);
        }

        Assert.assertEquals(N, list.size());

        for (int i = 0; i < N; i++) {
            Assert.assertEquals(N - i, list.get(i));
        }

        // add small list that would not need resizing of target
        DirectLongList list2 = new DirectLongList();
        list2.add(1001);
        list2.add(2001);

        list.add(list2);


        Assert.assertEquals(N + 2, list.size());
        Assert.assertEquals(1001, list.get(N));
        Assert.assertEquals(2001, list.get(N + 1));


        DirectLongList list3 = new DirectLongList();
        for (int i = 0; i < N; i++) {
            list3.add(i + 5000);
        }

        list.add(list3);
        Assert.assertEquals(2 * N + 2, list.size());
    }
}
