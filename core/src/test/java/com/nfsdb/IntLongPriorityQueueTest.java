/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.misc.Rnd;
import com.nfsdb.std.IntLongPriorityQueue;
import com.nfsdb.store.IndexCursor;
import com.nfsdb.store.KVIndex;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class IntLongPriorityQueueTest extends AbstractTest {
    private static final int totalKeys = 10;
    private static final int totalValues = 100;
    private File indexFile;

    @Before
    public void setUp() {
        indexFile = new File(factory.getConfiguration().getJournalBase(), "index-test");
    }

    @Test
    public void testIndexSort() throws Exception {
        final int nStreams = 16;
        Rnd rnd = new Rnd();
        int totalLen = 0;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
            for (int i = 0; i < nStreams; i++) {
                long values[] = new long[rnd.nextPositiveInt() % 1000];
                totalLen += values.length;

                for (int j = 0; j < values.length; j++) {
                    values[j] = rnd.nextPositiveLong() % 100;
                }

                Arrays.sort(values);

                for (int j = 0; j < values.length; j++) {
                    index.add(i, values[j]);
                }
                index.commit();
            }


            long expected[] = new long[totalLen];
            int p = 0;

            for (int i = 0; i < nStreams; i++) {
                IndexCursor c = index.fwdCursor(i);
                while (c.hasNext()) {
                    expected[p++] = c.next();
                }
            }

            Arrays.sort(expected);

            IntLongPriorityQueue heap = new IntLongPriorityQueue(nStreams);
            IndexCursor cursors[] = new IndexCursor[nStreams];

            for (int i = 0; i < nStreams; i++) {
                cursors[i] = index.newFwdCursor(i);

                if (cursors[i].hasNext()) {
                    heap.add(i, cursors[i].next());
                }
            }

            p = 0;
            while (heap.hasNext()) {
                int idx = heap.popIndex();
                long v;
                if (cursors[idx].hasNext()) {
                    v = heap.popAndReplace(idx, cursors[idx].next());
                } else {
                    v = heap.popValue();
                }
                Assert.assertEquals(expected[p++], v);
            }
        }
    }
}
