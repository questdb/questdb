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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointBinaryKeyIndex;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewCheckpointBinaryKeyIndexTest {
    private static final int KEY_COUNT = 50_000;

    @Test
    public void testCompositeQualifiersOverwriteAndHighCardinalityReuse() {
        final LiveViewCheckpointBinaryKeyIndex index = new LiveViewCheckpointBinaryKeyIndex();
        final byte[] sharedKey = key(42);
        index.put(1, 7, sharedKey, 11);
        index.put(2, 7, sharedKey, 22);
        index.put(1, 8, sharedKey, 33);
        index.put(1, 7, sharedKey, 44);
        Assert.assertEquals(44, index.get(1, 7, key(42)));
        Assert.assertEquals(22, index.get(2, 7, key(42)));
        Assert.assertEquals(33, index.get(1, 8, key(42)));
        Assert.assertEquals(-1, index.get(2, 8, key(42)));
        Assert.assertEquals(3, index.size());

        index.clear();
        Assert.assertEquals(0, index.size());
        for (int i = 0; i < KEY_COUNT; i++) {
            index.put(i & 7, i & 3, key(i), i);
        }
        Assert.assertEquals(KEY_COUNT, index.size());
        for (int i = KEY_COUNT - 1; i >= 0; i--) {
            Assert.assertEquals(i, index.get(i & 7, i & 3, key(i)));
        }

        index.clear();
        Assert.assertEquals(0, index.size());
        for (int i = 0; i < KEY_COUNT; i++) {
            Assert.assertEquals(-1, index.get(i & 7, i & 3, key(i)));
            index.put(i & 7, i & 3, key(i), KEY_COUNT - i);
        }
        Assert.assertEquals(KEY_COUNT, index.size());
        for (int i = 0; i < KEY_COUNT; i++) {
            Assert.assertEquals(KEY_COUNT - i, index.get(i & 7, i & 3, key(i)));
        }
    }

    private static byte[] key(int value) {
        return new byte[]{
                (byte) value,
                (byte) (value >>> 8),
                (byte) (value >>> 16),
                (byte) (value >>> 24),
                (byte) (value * 31)
        };
    }
}
