/*+*****************************************************************************
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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.ObjLongMatrix;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ObjLongMatrixTest {
    private static final int COLUMN_COUNT = 5;
    private static final Log LOG = LogFactory.getLog(ObjLongMatrixTest.class);

    @Test
    public void testApproxBinarySearch() {
        int k = 0;
        ObjLongMatrix<String> m = new ObjLongMatrix<>(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, k);
            m.set(r, 1, k);
            m.set(r, "s" + k);
            k += 2;
        }

        int r = m.binarySearch(631, 0);
        Assert.assertTrue(r < 0);
        Assert.assertEquals(632, m.get(-r - 1, 0));

        r = m.binarySearch(2500, 0);
        Assert.assertEquals(1000, -r - 1);
    }

    @Test
    public void testBinarySearch() {
        ObjLongMatrix<String> m = new ObjLongMatrix<>(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
            m.set(r, "s" + i);
        }

        int r = m.binarySearch(631, 0);
        Assert.assertEquals(631, m.get(r, 0));
    }

    @Test
    public void testDeleteMarked() {
        ObjLongMatrix<String> m = new ObjLongMatrix<>(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, -i);
            m.set(r, "s" + i);
        }

        Assert.assertEquals(0, m.deleteMarked());
        Assert.assertEquals(1000, m.size());

        // unsorted, across bitmap words and in the rows added by resizes, with adjacent rows
        // and a repeat
        final int[] marked = {999, 0, 63, 64, 65, 452, 450, 451, 511, 512, 700, 450};
        for (int r : marked) {
            m.markDeleted(r);
        }
        Assert.assertEquals(1000, m.size());
        Assert.assertTrue(m.isMarked(450));
        Assert.assertFalse(m.isMarked(453));
        Assert.assertEquals(450, m.binarySearch(450, 0));
        Assert.assertEquals("s450", m.get(450));

        Assert.assertEquals(11, m.deleteMarked());

        final boolean[] isRemoved = new boolean[1000];
        for (int r : marked) {
            isRemoved[r] = true;
        }
        Assert.assertEquals(989, m.size());
        int row = 0;
        for (int i = 0; i < 1000; i++) {
            if (!isRemoved[i]) {
                Assert.assertEquals(i, m.get(row, 0));
                Assert.assertEquals(-i, m.get(row, 1));
                Assert.assertEquals("s" + i, m.get(row));
                Assert.assertFalse(m.isMarked(row));
                row++;
            }
        }

        Assert.assertEquals(0, m.deleteMarked());
        Assert.assertEquals(989, m.size());
    }

    @Test
    public void testDeleteMarkedAcrossResize() {
        // the matrix starts with room for 512 rows, so appending the 513th row resizes it while
        // rows are marked
        final ObjLongMatrix<String> m = new ObjLongMatrix<>(COLUMN_COUNT);
        final LongList expected = new LongList();
        for (int i = 0; i < 512; i++) {
            appendRow(m, expected, i);
        }
        m.markDeleted(0);
        m.markDeleted(100);
        m.markDeleted(511);
        for (int i = 512; i < 1100; i++) {
            appendRow(m, expected, i);
        }
        m.markDeleted(512);
        m.markDeleted(1023);
        m.markDeleted(1099);
        Assert.assertTrue(m.isMarked(100));
        Assert.assertTrue(m.isMarked(511));
        Assert.assertTrue(m.isMarked(1099));
        Assert.assertFalse(m.isMarked(101));

        Assert.assertEquals(6, m.deleteMarked());
        for (int r : new int[]{1099, 1023, 512, 511, 100, 0}) {
            expected.removeIndex(r);
        }
        assertRows(m, expected);
        for (int r = 0, n = m.size(); r < n; r++) {
            Assert.assertFalse(m.isMarked(r));
        }
    }

    @Test
    public void testDeleteMarkedFuzz() {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final ObjLongMatrix<String> m = new ObjLongMatrix<>(COLUMN_COUNT);
        final LongList expected = new LongList();
        final LongList kept = new LongList();
        long nextId = 0;
        for (int i = 0, n = 1000 + rnd.nextInt(2000); i < n; i++) {
            appendRow(m, expected, nextId++);
        }
        for (int round = 0; round < 500; round++) {
            final int size = m.size();
            final boolean[] isRowMarked = new boolean[size];
            int markedCount = 0;
            for (int i = 0, n = size > 0 ? rnd.nextInt(Math.min(size, 300) + 1) : 0; i < n; i++) {
                final int r = rnd.nextInt(size);
                m.markDeleted(r);
                Assert.assertTrue(m.isMarked(r));
                if (!isRowMarked[r]) {
                    isRowMarked[r] = true;
                    markedCount++;
                }
                if (rnd.nextInt(10) == 0) {
                    // rows appended between marks, as accept() appends them during a poll round
                    appendRow(m, expected, nextId++);
                }
            }
            Assert.assertEquals(markedCount, m.deleteMarked());

            kept.clear();
            for (int i = 0, n = expected.size(); i < n; i++) {
                if (i >= size || !isRowMarked[i]) {
                    kept.add(expected.getQuick(i));
                }
            }
            expected.clear();
            expected.add(kept);
            assertRows(m, expected);

            for (int i = 0, n = rnd.nextInt(300); i < n; i++) {
                appendRow(m, expected, nextId++);
            }
        }
    }

    @Test
    public void testDeleteRow() {
        ObjLongMatrix<String> m = new ObjLongMatrix<>(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
            m.set(r, "s" + i);
        }

        Assert.assertEquals(1000, m.size());

        m.deleteRow(450);

        Assert.assertEquals(999, m.size());

        Assert.assertEquals(451, m.get(450, 0));
        Assert.assertEquals(451, m.get(450, 1));
        Assert.assertEquals("s" + 451, m.get(450));

        Assert.assertEquals(449, m.get(449, 0));
        Assert.assertEquals(449, m.get(449, 1));
        Assert.assertEquals("s" + 449, m.get(449));

        m.deleteRow(998);
        Assert.assertEquals(998, m.size());

        Assert.assertEquals(998, m.get(997, 0));
        Assert.assertEquals(998, m.get(997, 1));
        Assert.assertEquals("s" + 998, m.get(997));

        m.deleteRow(0);
        Assert.assertEquals(997, m.size());
        Assert.assertEquals(998, m.get(996, 0));
        Assert.assertEquals(998, m.get(996, 1));
        Assert.assertEquals("s" + 998, m.get(996));
    }

    @Test
    public void testResize() {
        ObjLongMatrix<String> m = new ObjLongMatrix<>(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
            m.set(r, "s" + i);
        }
        Assert.assertEquals(1000, m.size());

        for (int i = 0, n = m.size(); i < n; i++) {
            Assert.assertEquals(i, m.get(i, 0));
            Assert.assertEquals(i, m.get(i, 1));
            Assert.assertEquals("s" + i, m.get(i));
        }
    }

    @Test
    public void testZapTop() {
        ObjLongMatrix<String> m = new ObjLongMatrix<>(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
            m.set(r, "s" + i);
        }

        Assert.assertEquals(1000, m.size());
        m.zapTop(35);
        Assert.assertEquals(965, m.size());

        Assert.assertEquals(35, m.get(0, 0));
        Assert.assertEquals(35, m.get(0, 1));
        Assert.assertEquals("s" + 35, m.get(0));

        Assert.assertEquals(999, m.get(964, 0));
        Assert.assertEquals(999, m.get(964, 1));
        Assert.assertEquals("s" + 999, m.get(964));

        m.zapTop(1000);
        Assert.assertEquals(0, m.size());
    }

    private static void appendRow(ObjLongMatrix<String> m, LongList expected, long id) {
        final int r = m.addRow();
        for (int c = 0; c < COLUMN_COUNT; c++) {
            m.set(r, c, id * COLUMN_COUNT + c);
        }
        m.set(r, "s" + id);
        expected.add(id);
    }

    private static void assertRows(ObjLongMatrix<String> m, LongList expected) {
        Assert.assertEquals(expected.size(), m.size());
        for (int r = 0, n = expected.size(); r < n; r++) {
            final long id = expected.getQuick(r);
            for (int c = 0; c < COLUMN_COUNT; c++) {
                Assert.assertEquals(id * COLUMN_COUNT + c, m.get(r, c));
            }
            Assert.assertEquals("s" + id, m.get(r));
        }
    }
}
