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

import io.questdb.std.LongMatrix;
import org.junit.Assert;
import org.junit.Test;

public class LongMatrixTest {

    @Test
    public void testApproxBinarySearch() {
        int k = 0;
        LongMatrix m = new LongMatrix(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, k);
            m.set(r, 1, k);
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
        LongMatrix m = new LongMatrix(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
        }

        int r = m.binarySearch(631, 0);
        Assert.assertEquals(631, m.get(r, 0));
    }

    @Test
    public void testDeleteRow() {
        LongMatrix m = new LongMatrix(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
        }

        Assert.assertEquals(1000, m.size());

        m.deleteRow(450);

        Assert.assertEquals(999, m.size());

        Assert.assertEquals(451, m.get(450, 0));
        Assert.assertEquals(451, m.get(450, 1));

        Assert.assertEquals(449, m.get(449, 0));
        Assert.assertEquals(449, m.get(449, 1));

        m.deleteRow(998);
        Assert.assertEquals(998, m.size());

        Assert.assertEquals(998, m.get(997, 0));
        Assert.assertEquals(998, m.get(997, 1));

        m.deleteRow(0);
        Assert.assertEquals(997, m.size());
        Assert.assertEquals(998, m.get(996, 0));
        Assert.assertEquals(998, m.get(996, 1));
    }

    @Test
    public void testResize() {
        LongMatrix m = new LongMatrix(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
        }
        Assert.assertEquals(1000, m.size());

        for (int i = 0, n = m.size(); i < n; i++) {
            Assert.assertEquals(i, m.get(i, 0));
            Assert.assertEquals(i, m.get(i, 1));
        }
    }

    @Test
    public void testZapTop() {
        LongMatrix m = new LongMatrix(2);
        for (int i = 0; i < 1000; i++) {
            int r = m.addRow();
            m.set(r, 0, i);
            m.set(r, 1, i);
        }

        Assert.assertEquals(1000, m.size());
        m.zapTop(35);
        Assert.assertEquals(965, m.size());

        Assert.assertEquals(35, m.get(0, 0));
        Assert.assertEquals(35, m.get(0, 1));

        Assert.assertEquals(999, m.get(964, 0));
        Assert.assertEquals(999, m.get(964, 1));

        m.zapTop(1000);
        Assert.assertEquals(0, m.size());
    }
}
