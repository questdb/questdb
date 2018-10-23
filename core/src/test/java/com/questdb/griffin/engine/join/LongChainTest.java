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

package com.questdb.griffin.engine.join;

import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LongChainTest {

    @Test
    public void testAll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LongChain chain = new LongChain(1024 * 1024)) {
                final int N = 1000;
                final int nChains = 10;
                final Rnd rnd = new Rnd();
                final LongList heads = new LongList(nChains);
                final ObjList<LongList> expectedValues = new ObjList<>();

                for (int i = 0; i < nChains; i++) {
                    LongList expected = new LongList(N);
                    heads.add(populateChain(chain, rnd, expected));
                    expectedValues.add(expected);
                    Assert.assertEquals(N, expected.size());
                }
                Assert.assertEquals(nChains, expectedValues.size());

                // values are be in reverse order
                for (int i = 0; i < nChains; i++) {
                    LongChain.TreeCursor cursor = chain.getCursor(heads.getQuick(i));
                    LongList expected = expectedValues.get(i);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(expected.getQuick(count), cursor.next());
                        count++;
                    }
                    Assert.assertEquals(N, count);
                }
            }
        });
    }

    private long populateChain(LongChain chain, Rnd rnd, LongList expectedValues) {
        long head = -1;
        long tail = -1;
        for (int i = 0; i < 1000; i++) {
            long expected = rnd.nextLong();
            tail = chain.put(tail, expected);
            expectedValues.add(expected);
            if (i == 0) {
                head = tail;
            }
        }
        return head;
    }
}