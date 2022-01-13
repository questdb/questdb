/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine;

import io.questdb.griffin.engine.table.LongTreeSet;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LongTreeSetTest {
    // To instantiate a logger to prevent memory leaks from being detected
    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(LongTreeSetTest.class);

    @Test
    public void testDuplicateValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LongTreeSet set = new LongTreeSet(1024, 1)) {
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
            try (LongTreeSet set = new LongTreeSet(1024 * 1024, 1)) {
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