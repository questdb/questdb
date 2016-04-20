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
import com.questdb.store.UnstructuredFile;
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

        Rnd expected = new Rnd();

        hb.setPos(pos);

        for (int i = 0; i < 10000; i++) {
            hb.get(vals);
            for (int k = 0; k < vals.length; k++) {
                Assert.assertEquals(expected.nextInt(), vals[k]);
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

        Rnd expected = new Rnd();

        hb.setPos(pos);

        for (int i = 0; i < 10000; i++) {
            hb.get(vals);
            for (int k = 0; k < vals.length; k++) {
                Assert.assertEquals(expected.nextLong(), vals[k]);
            }
        }
    }

}
