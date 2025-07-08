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

package io.questdb.test.cairo;

import io.questdb.cairo.DataID;
import io.questdb.cairo.DataIDFactory;
import io.questdb.std.Long256Impl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DataIDTest extends AbstractCairoTest {
    @Test
    public void testOpenDataID() throws Exception {
        assertMemoryLeak(() -> {
            try (DataID id = DataIDFactory.open(engine.getConfiguration())) {
                Assert.assertNotEquals(Long256Impl.ZERO_LONG256, id.get());
            }
        });
    }

    @Test
    public void testSetDataID() throws Exception {
        assertMemoryLeak(() -> {
            try (DataID id = DataIDFactory.open(engine.getConfiguration())) {
                Long256Impl previous = id.get();
                Long256Impl newId = new Long256Impl();
                newId.fromRnd(engine.getConfiguration().getRandom());
                Assert.assertNotEquals(newId, previous);
                id.set(newId);
                Assert.assertEquals(newId, id.get());
            }
        });
    }
}