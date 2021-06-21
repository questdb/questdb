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

package io.questdb.griffin.engine.table;

import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LatestByArgumentsTest {
    @Test
    public void testLatestByArguments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long address = LatestByArguments.allocateMemory();
            LatestByArguments.setKeyLo(address,1);
            LatestByArguments.setKeyHi(address, 2);
            LatestByArguments.setRowsAddress(address, 3);
            LatestByArguments.setRowsCapacity(address, 4);
            LatestByArguments.setRowsSize(address, 5);

            assertEquals(5, LatestByArguments.getRowsSize(address));
            assertEquals(4, LatestByArguments.getRowsCapacity(address));
            assertEquals(3, LatestByArguments.getRowsAddress(address));
            assertEquals(2, LatestByArguments.getKeyHi(address));
            assertEquals(1, LatestByArguments.getKeyLo(address));
            LatestByArguments.releaseMemory(address);
        });
    }

    @Test
    public void testLatestByArgumentsArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int elements = 128;
            long baseAddress = LatestByArguments.allocateMemoryArray(elements);
            for(int i = 0; i < elements; ++i) {
                final long address = baseAddress + i * LatestByArguments.MEMORY_SIZE;
                LatestByArguments.setKeyLo(address, 1);
                LatestByArguments.setKeyHi(address, 2);
                LatestByArguments.setRowsAddress(address, 3);
                LatestByArguments.setRowsCapacity(address, 4);
                LatestByArguments.setRowsSize(address, 5);
            }

            for(int i = 0; i < elements; ++i) {
                final long address = baseAddress + i * LatestByArguments.MEMORY_SIZE;
                assertEquals(5, LatestByArguments.getRowsSize(address));
                assertEquals(4, LatestByArguments.getRowsCapacity(address));
                assertEquals(3, LatestByArguments.getRowsAddress(address));
                assertEquals(2, LatestByArguments.getKeyHi(address));
                assertEquals(1, LatestByArguments.getKeyLo(address));
            }
            LatestByArguments.releaseMemoryArray(baseAddress, elements);
        });
    }
}
