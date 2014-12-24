/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal;

import com.nfsdb.journal.collections.DirectCompositeKeyIntMap;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.utils.Rnd;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class DirectMapTest {
    @Test
    @Ignore
    public void testCompositeKeyMap() throws Exception {
        DirectCompositeKeyIntMap map = new DirectCompositeKeyIntMap(new ColumnType[]{ColumnType.LONG, ColumnType.STRING});
        Rnd rnd = new Rnd();

        for (int i = 0; i < 10000; i++) {
            map.put(
                    map.withKey()
                            .putLong(rnd.nextLong())
                            .putStr(rnd.nextString(10))
                            .$()
                    , i
            );
        }


        int count = 0;
        for (DirectCompositeKeyIntMap.Entry e : map) {
            count++;
            e.key.getLong(0);
            e.key.getStr(1);
        }

        Assert.assertEquals(10000, count);
    }
}
