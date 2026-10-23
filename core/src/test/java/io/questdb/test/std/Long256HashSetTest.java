/*+*****************************************************************************
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

import io.questdb.std.Long256HashSet;
import io.questdb.std.Long256Impl;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class Long256HashSetTest {

    @Test
    public void testAddAndContainsAcrossRehash() {
        final Rnd rnd = new Rnd();
        final Long256HashSet set = new Long256HashSet(4, 0.5);
        final Long256Impl value = new Long256Impl();
        for (int i = 0; i < 10_000; i++) {
            value.setAll(rnd.nextLong(), rnd.nextLong(), i, 0);
            set.add(value);
            set.add(value);
        }
        final Rnd replay = new Rnd();
        for (int i = 0; i < 10_000; i++) {
            value.setAll(replay.nextLong(), replay.nextLong(), i, 0);
            Assert.assertTrue(set.contains(value));
            value.setAll(value.getLong0(), value.getLong1(), i, 1);
            Assert.assertFalse(set.contains(value));
        }
    }

    @Test
    public void testNull() {
        final Long256HashSet set = new Long256HashSet(4, 0.5);
        final Long256Impl value = new Long256Impl();
        value.setAll(1, 0, 0, 0);
        set.add(value);
        Assert.assertFalse(set.contains(Long256Impl.NULL_LONG256));
        set.add(Long256Impl.NULL_LONG256);
        Assert.assertTrue(set.contains(Long256Impl.NULL_LONG256));
        Assert.assertTrue(set.contains(value));

        set.clear();
        Assert.assertFalse(set.contains(Long256Impl.NULL_LONG256));
        Assert.assertFalse(set.contains(value));
    }

    @Test
    public void testToSink() {
        final Long256HashSet set = new Long256HashSet(4, 0.5);
        final Long256Impl value = new Long256Impl();
        value.setAll(1, 0, 0, Long.MIN_VALUE);
        set.add(value);
        set.add(Long256Impl.NULL_LONG256);
        final StringSink sink = new StringSink();
        set.toSink(sink);
        TestUtils.assertEquals("[0x8000000000000000000000000000000000000000000000000000000000000001]", sink);
    }
}
