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

package io.questdb.test.griffin.engine.table;

import io.questdb.griffin.engine.table.AsyncHorizonTimestampIterator;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class AsyncHorizonTimestampIteratorTest {

    @Test
    public void testLiveFieldsDoNotUseObjectBoundaryCacheLine() {
        for (Field field : AsyncHorizonTimestampIterator.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                final long offset = Unsafe.objectFieldOffset(field);
                Assert.assertTrue(
                        "live field is within the object boundary cache line [field="
                                + field.getName() + ", offset=" + offset + ']',
                        offset >= Misc.CACHE_LINE_SIZE
                );
            }
        }
    }
}
