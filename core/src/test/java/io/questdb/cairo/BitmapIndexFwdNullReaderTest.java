/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class BitmapIndexFwdNullReaderTest {

    private static final BitmapIndexFwdNullReader reader = new BitmapIndexFwdNullReader();

    @Test
    public void testAlwaysOpen() {
        Assert.assertTrue(reader.isOpen());
    }

    @Test
    public void testCursor() {
        final Rnd rnd = new Rnd();
        for (int i = 0; i < 10; i++) {
            int n = rnd.nextPositiveInt() % 1024;
            int m = 0;
            RowCursor cursor = reader.getCursor(true, 0, 0, n);
            while (cursor.hasNext()) {
                Assert.assertEquals(m++, cursor.next());
            }

            Assert.assertEquals(n + 1, m);
        }
    }

    @Test
    public void testKeyCount() {
        // has to be always 1
        Assert.assertEquals(1, reader.getKeyCount());
    }

    @Test
    public void testMemoryGetters() {
        Assert.assertEquals(0, reader.getKeyBaseAddress());
        Assert.assertEquals(0, reader.getKeyMemorySize());
        Assert.assertEquals(0, reader.getValueBaseAddress());
        Assert.assertEquals(0, reader.getValueMemorySize());
        Assert.assertEquals(0, reader.getUnIndexedNullCount());
        Assert.assertEquals(0, reader.getValueBlockCapacity());
    }

}