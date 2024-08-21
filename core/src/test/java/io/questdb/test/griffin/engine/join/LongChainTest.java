/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.join;

import io.questdb.griffin.engine.join.LongChain;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class LongChainTest {
    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(LongChainTest.class);

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> {
            try (LongChain chain = new LongChain(1024, Integer.MAX_VALUE)) {
                final int N = 1000;
                final int nChains = 10;
                final Rnd rnd = new Rnd();
                final IntList heads = new IntList(nChains);
                final ObjList<LongList> expectedValues = new ObjList<>();

                for (int i = 0; i < nChains; i++) {
                    LongList expected = new LongList(N);
                    heads.add(populateChain(chain, rnd, expected));
                    expectedValues.add(expected);
                    Assert.assertEquals(N, expected.size());
                }
                Assert.assertEquals(nChains, expectedValues.size());

                // values are expected in reverse order
                for (int i = 0; i < nChains; i++) {
                    LongChain.Cursor cursor = chain.getCursor(heads.getQuick(i));
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

    @Test
    public void testCompressOffset() {
        Assert.assertEquals(0, LongChain.compressOffset(0));
        Assert.assertEquals(1 / LongChain.CHAIN_VALUE_SIZE, LongChain.compressOffset(1));
        for (long i = 0; i < 1000; i++) {
            Assert.assertEquals(i, LongChain.compressOffset(i * LongChain.CHAIN_VALUE_SIZE));
        }
        Assert.assertEquals(Integer.MAX_VALUE, LongChain.compressOffset(Integer.MAX_VALUE * LongChain.CHAIN_VALUE_SIZE));
    }

    private int populateChain(LongChain chain, Rnd rnd, LongList expectedValues) {
        int head = -1;
        int tail = -1;
        for (int i = 0; i < 1000; i++) {
            long expected = rnd.nextLong();
            tail = chain.put(expected, tail);
            expectedValues.add(expected);
            if (i == 0) {
                head = tail;
            }
        }
        return head;
    }
}