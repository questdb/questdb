/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.store;

import com.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UnstructuredFileTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testIntArrays() throws Exception {
        Rnd rnd = new Rnd();
        UnstructuredFile hb = new UnstructuredFile(temp.newFile(), 16, JournalMode.APPEND);

        int vals[] = new int[100];

        long pos = hb.getPos();

        for (int i = 0; i < 10000; i++) {
            for (int k = 0; k < vals.length; k++) {
                vals[k] = rnd.nextInt();
            }
            hb.put(vals);
        }


        rnd.reset();
        hb.setPos(pos);

        for (int i = 0; i < 10000; i++) {
            hb.get(vals);
            for (int k = 0; k < vals.length; k++) {
                Assert.assertEquals(rnd.nextInt(), vals[k]);
            }
        }
    }

    @Test
    public void testLongArrays() throws Exception {
        Rnd rnd = new Rnd();
        UnstructuredFile hb = new UnstructuredFile(temp.newFile(), 16, JournalMode.APPEND);

        long vals[] = new long[100];

        long pos = hb.getPos();

        for (int i = 0; i < 10000; i++) {
            for (int k = 0; k < vals.length; k++) {
                vals[k] = rnd.nextLong();
            }
            hb.put(vals);
        }


        rnd.reset();
        hb.setPos(pos);

        for (int i = 0; i < 10000; i++) {
            hb.get(vals);
            for (int k = 0; k < vals.length; k++) {
                Assert.assertEquals(rnd.nextLong(), vals[k]);
            }
        }
    }

}
