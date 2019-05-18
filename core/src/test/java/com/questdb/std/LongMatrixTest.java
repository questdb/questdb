/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std;

import org.junit.Assert;
import org.junit.Test;

public class LongMatrixTest {

    @Test
    public void testApproxBinarySearch() {
        int k = 0;
        LongMatrix<String> m = new LongMatrix<>(2);
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
        LongMatrix<String> m = new LongMatrix<>(2);
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
    public void testDeleteRow() {
        LongMatrix<String> m = new LongMatrix<>(2);
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
        LongMatrix<String> m = new LongMatrix<>(2);
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
        LongMatrix<String> m = new LongMatrix<>(2);
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
}