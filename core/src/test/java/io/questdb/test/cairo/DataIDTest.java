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
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Uuid;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DataIDTest extends AbstractCairoTest {
    @Test
    public void testOpenDataID() throws Exception {
        assertMemoryLeak(() -> {
            DataID id = DataID.open(configuration);
            Assert.assertNotNull(id);
            Assert.assertFalse(id.isInitialized());
            Assert.assertEquals(Numbers.LONG_NULL, id.getLo());
            Assert.assertEquals(Numbers.LONG_NULL, id.getHi());

            Rnd rnd = new Rnd(configuration.getMicrosecondClock().getTicks(), configuration.getMillisecondClock().getTicks());
            Uuid currentId = new Uuid();
            currentId.of(rnd.nextLong(), rnd.nextLong());
            id.set(currentId.getLo(), currentId.getHi());
            Assert.assertTrue(id.isInitialized());
            Assert.assertEquals(id.getLo(), currentId.getLo());
            Assert.assertEquals(id.getHi(), currentId.getHi());

            DataID updatedId = DataID.open(configuration);
            Assert.assertTrue(updatedId.isInitialized());
            Assert.assertEquals(updatedId.getLo(), currentId.getLo());
            Assert.assertEquals(updatedId.getHi(), currentId.getHi());

            // Ensure that the data is still there
            updatedId = DataID.open(configuration);
            Assert.assertTrue(updatedId.isInitialized());
            Assert.assertEquals(updatedId.getLo(), currentId.getLo());
            Assert.assertEquals(updatedId.getHi(), currentId.getHi());
        });
    }

    @Test
    public void testInvalidDataID() throws Exception {
        assertMemoryLeak(() -> {
            // Creates a file of 8 bytes instead of 16
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot());
                path.concat(DataID.FILENAME);

                final FilesFacade ff = configuration.getFilesFacade();
                try (var mem = new MemoryCMARWImpl(ff, path.$(), 8, -1, MemoryTag.MMAP_DEFAULT, configuration.getWriterFileOpenOpts())) {
                    mem.putLong(0, 123);
                    mem.sync(false);
                }
            }

            DataID id = DataID.open(configuration);
            Assert.assertNotNull(id);
            Assert.assertFalse(id.isInitialized());
            Assert.assertEquals(Numbers.LONG_NULL, id.getLo());
            Assert.assertEquals(Numbers.LONG_NULL, id.getHi());
        });
    }
}