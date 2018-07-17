/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb;

import com.questdb.std.IntLongPriorityQueue;
import com.questdb.std.Rnd;
import com.questdb.store.IndexCursor;
import com.questdb.store.JournalMode;
import com.questdb.store.KVIndex;
import com.questdb.test.tools.AbstractTest;
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
        indexFile = new File(factoryContainer.getConfiguration().getJournalBase(), "index-test");
    }

    @Test
    public void testIndexSort() throws Exception {
        final int nStreams = 16;
        Rnd rnd = new Rnd();
        int totalLen = 0;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
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

            IntLongPriorityQueue heap = new IntLongPriorityQueue();
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
