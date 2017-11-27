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

package com.questdb.cairo;

import com.questdb.std.IntList;
import com.questdb.std.IntObjHashMap;
import com.questdb.std.LongList;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BitmapIndexTest extends AbstractCairoTest {
    @Test
    public void testAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration.getFilesFacade(), root, "x", 4)) {
                writer.add(0, 1000);
                writer.add(256, 1234);
                writer.add(64, 10);
                writer.add(64, 987);
                writer.add(256, 5567);
                writer.add(64, 91);
                writer.add(64, 92);
                writer.add(64, 93);
            }

            try (BitmapIndexBackwardReader reader = new BitmapIndexBackwardReader(configuration.getFilesFacade(), root, "x")) {
                LongList list = new LongList();
                assertThat("[5567,1234]", reader.getCursor(256), list);
                assertThat("[93,92,91,987,10]", reader.getCursor(64), list);
                assertThat("[1000]", reader.getCursor(0), list);
            }
        });
    }

    @Test
    public void testAdd1MValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int maxKeys = 1024;
            int N = 1000000;

            IntList keys = new IntList();
            IntObjHashMap<LongList> lists = new IntObjHashMap<>();

            // we need to have a conventional structure, which will unfortunately be memory-inefficient, to
            // assert that index is populated correctly
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration.getFilesFacade(), root, "x", 128)) {
                for (int i = 0; i < N; i++) {
                    int key = rnd.nextPositiveInt() % maxKeys;
                    long value = rnd.nextLong();
                    writer.add(key, value);

                    LongList list = lists.get(key);
                    if (list == null) {
                        lists.put(key, list = new LongList());
                        keys.add(key);
                    }
                    list.add(value);
                }
            }

            // read values and compare to the structure index is expected to be holding
            try (BitmapIndexBackwardReader reader = new BitmapIndexBackwardReader(configuration.getFilesFacade(), root, "x")) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    LongList list = lists.get(keys.getQuick(i));
                    Assert.assertNotNull(list);

                    BitmapIndexCursor cursor = reader.getCursor(keys.getQuick(i));
                    int z = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertTrue(z > -1);
                        Assert.assertEquals(list.getQuick(z - 1), cursor.next());
                        z--;
                    }

                    // makes sure entire list is processed
                    Assert.assertEquals(0, z);
                }
            }
        });
    }

    private void assertThat(String expected, BitmapIndexCursor cursor, LongList temp) {
        temp.clear();
        while (cursor.hasNext()) {
            temp.add(cursor.next());
        }
        Assert.assertEquals(expected, temp.toString());
    }
}