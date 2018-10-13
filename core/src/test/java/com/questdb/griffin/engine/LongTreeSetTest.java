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

package com.questdb.griffin.engine;

import com.questdb.griffin.engine.table.LongTreeSet;
import com.questdb.std.LongList;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LongTreeSetTest {
    @Test
    public void testDuplicateValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LongTreeSet set = new LongTreeSet(1024)) {
                set.put(1);
                set.put(2);
                set.put(2);
                set.put(3);
                set.put(4);
                set.put(4);
                set.put(5);
                set.put(5);
                set.put(5);

                LongTreeSet.TreeCursor cursor = set.getCursor();
                LongList ll = new LongList();
                while (cursor.hasNext()) {
                    ll.add(cursor.next());
                }
                TestUtils.assertEquals("[1,2,3,4,5]", ll.toString());
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LongTreeSet set = new LongTreeSet(1024 * 1024)) {
                doTestSimple(set);
                set.clear();
                doTestSimple(set);
            }
        });
    }

    private void doTestSimple(LongTreeSet set) {
        Rnd rnd = new Rnd();
        LongList ll = new LongList();
        int n = 10000;
        for (int i = 0; i < n; i++) {
            long l = rnd.nextLong();
            set.put(l);
            ll.add(l);
        }

        ll.sort();
        int i = 0;
        LongTreeSet.TreeCursor cursor = set.getCursor();
        while (cursor.hasNext()) {
            long l = cursor.next();
            Assert.assertEquals(ll.getQuick(i++), l);
        }
        Assert.assertEquals(n, i);

        cursor.toTop();

        i = 0;
        while (cursor.hasNext()) {
            long l = cursor.next();
            Assert.assertEquals(ll.getQuick(i++), l);
        }
    }
}