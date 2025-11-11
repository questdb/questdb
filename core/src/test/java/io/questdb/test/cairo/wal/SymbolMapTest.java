/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.SymbolMap;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class SymbolMapTest {
    @Test
    public void overrideValue() {
        try (SymbolMap map = new SymbolMap()) {
            Assert.assertEquals(SymbolMap.NO_ENTRY_VALUE, map.get("a"));
            map.put("a", 1);
            Assert.assertEquals(1, map.get("a"));
            map.put("a", 2);
            Assert.assertEquals(2, map.get("a"));
        }
    }

    @Test
    public void testBasic() {
        final int N = 10000;
        try (SymbolMap map = new SymbolMap()) {
            final Rnd rnd = new Rnd();

            int inserted = 0;
            for (int i = 0; i < N; i++) {
                final CharSequence key = rnd.nextChars(8);
                final int value = rnd.nextInt();
                final int hashCode = Chars.hashCode(key);
                final int index = map.keyIndex(key, hashCode);
                if (index > -1) {
                    map.putAt(index, key, value, hashCode);
                }
                Assert.assertEquals(value, map.get(key));
                inserted++;
            }
            Assert.assertEquals(inserted, map.size());

            map.clear();

            Assert.assertEquals(0, map.size());
        }
    }

    @Test
    public void testOffsets() {
        try (SymbolMap map = new SymbolMap()) {
            Rnd rnd = new Rnd();
            final ObjList<CharSequence> keys = new ObjList<>(1000);
            for (int i = 0; i < 1000; i++) {
                CharSequence key = rnd.nextChars(32).toString() + i;
                keys.add(key);
                map.put(key, i);
            }
            Assert.assertEquals(1000, map.size());

            for (int i = 0, offset = map.nextOffset(); i < keys.size(); i++, offset = map.nextOffset(offset)) {
                Assert.assertTrue(offset >= 0);
                CharSequence key = keys.getQuick(i);
                Assert.assertEquals(key, map.get(offset).toString());
            }
        }

    }
}