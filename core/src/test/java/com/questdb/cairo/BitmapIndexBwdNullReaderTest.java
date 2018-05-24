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

package com.questdb.cairo;

import com.questdb.common.RowCursor;
import com.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class BitmapIndexBwdNullReaderTest {

    private static final BitmapIndexBwdNullReader reader = new BitmapIndexBwdNullReader();

    @Test
    public void testAlwaysOpen() {
        Assert.assertTrue(reader.isOpen());
    }

    @Test
    public void testCursor() {
        final Rnd rnd = new Rnd();
        for (int i = 0; i < 10; i++) {
            int n = rnd.nextPositiveInt() % 1024;
            int m = n;
            RowCursor cursor = reader.getCursor(true, 0, 0, n);
            while (cursor.hasNext()) {
                Assert.assertEquals(m--, cursor.next());
            }

            Assert.assertEquals(-1, m);
        }
    }

    @Test
    public void testKeyCount() {
        // has to be always 1
        Assert.assertEquals(1, reader.getKeyCount());
    }
}