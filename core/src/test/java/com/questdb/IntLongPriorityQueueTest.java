/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb;

import com.questdb.misc.Rnd;
import com.questdb.std.IntLongPriorityQueue;
import com.questdb.store.IndexCursor;
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
