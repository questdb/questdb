/*******************************************************************************
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

import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ObjListTest {

    @Test
    public void testContains() {
        Assert.assertTrue(list("a", "b", "c").contains("a"));
        Assert.assertTrue(list("a", "b", "c").contains("b"));
        Assert.assertTrue(list("a", "b", "c").contains("c"));
        Assert.assertTrue(list("a", "b", "c", null).contains(null));
        Assert.assertTrue(list("a", "b", "c", "").contains(""));

        Assert.assertFalse(list().contains("a"));
        Assert.assertFalse(list().contains(null));
        Assert.assertFalse(list().contains(""));
        Assert.assertFalse(list("a", "b", "c").contains("d"));
        Assert.assertFalse(list("a", "b", "c").contains(""));
        Assert.assertFalse(list("a", "b", "c").contains(null));
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(list("a"), list("a"));
        Assert.assertEquals(list("a", null, "b"), list("a", null, "b"));
        Assert.assertEquals(list("a", "b", "c"), list("a", "b", "c"));

        Assert.assertEquals(list(), list());

        Assert.assertNotEquals(list("a"), list("b"));
        Assert.assertNotEquals(list("a"), list("a", "b"));
        Assert.assertNotEquals(list("a", null), list("a", "b"));
        Assert.assertNotEquals(list("a"), list());
    }

    @Test
    public void testRemoveFromTo() {
        Assert.assertEquals(list("a"), remove(list("a", "b", "c"), 1, 2));
        Assert.assertEquals(list("a", "c"), remove(list("a", "b", "c"), 1, 1));
        Assert.assertEquals(list("b", "c"), remove(list("a", "b", "c"), 0, 0));
        Assert.assertEquals(list("c"), remove(list("a", "b", "c"), 0, 1));
        Assert.assertEquals(list(), remove(list("a", "b", "c"), 0, 2));

        Assert.assertEquals(list(), remove(list("a", "b", "c"), 0, 20));
        Assert.assertEquals(list(), remove(list("a", "b", "c"), 4, 10));
    }

    private static <T> ObjList<T> remove(ObjList<T> o, int from, int to) {
        o.remove(from, to);
        return o;
    }

    private ObjList<String> list(String... values) {
        ObjList<String> result = new ObjList<>();
        for (String value : values) {
            result.add(value);
        }
        return result;
    }
}
